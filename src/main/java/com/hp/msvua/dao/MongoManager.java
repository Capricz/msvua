package com.hp.msvua.dao;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.hp.msvua.model.ResultSetDTO;
import com.hp.msvua.util.Configuration;
import com.hp.msvua.util.Const;
import com.hp.msvua.util.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoManager implements Serializable {
	
	private static final long serialVersionUID = 1L;

	static Logger logger = Logger.getLogger(MongoManager.class);
	
	public static final String SEPARATOR = ",";

	private Mongo mongoClient;
	private DB db;
	DBObject query = new BasicDBObject().append(Const.FIELD_FLAG, -1);
	DBObject sort = new BasicDBObject(Const.FIELD_TIME, Const.SORT_ORDER_ASC);
	public int fetchBatchSize;
	public String COLLECTION_NAMES;
	
	public MongoManager(){
		logger.debug("in MongoManager - constructor");
		try {
			initialize();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public void initialize() throws UnknownHostException {
		String host = Configuration.getValue(Const.MONGODB_HOST);
		logger.debug("mongodb loading... host : "+host);
		int port = Configuration.getValueAsInteger(Const.MONGODB_PORT);
		logger.debug("mongodb loading... port :"+port);
		String dbName = Configuration.getValue(Const.MONGODB_DBNAME);
		logger.debug("mongodb loading... dbName : "+dbName);
		String username = Configuration.getValue(Const.MONGODB_USERNAME);
		logger.debug("mongodb loading... username :"+username);
		String password = Configuration.getValue(Const.MONGODB_PASSWORD);
		logger.debug("mongodb loading... password : "+password);
		logger.debug("in MongoManager - initialize");
		mongoClient = new Mongo( host , port );
		logger.info("in MongoManager - new mongoClient...");
		db = mongoClient.getDB(dbName);
		db.authenticate(username, password.toCharArray());
		logger.debug("in MongoManager - new db...");
		BasicDBList or = new BasicDBList();
		or.add(new BasicDBObject(Const.FIELD_FLAG,new BasicDBObject("$exists",false)));
		or.add(new BasicDBObject(Const.FIELD_FLAG,Const.STATUS_DRAFT));
		query = new BasicDBObject("$or",or);
		COLLECTION_NAMES = Configuration.getValue(Const.MONGODB_COLLECTION_NAMES);
		fetchBatchSize = Configuration.getValueAsInteger(Const.MONGODB_FETCH_BATCH_SIZE);
//		printCollection(collection);
	}
	
	public void close() {
		logger.info("in MongoManager - close mongoClient...");
		mongoClient.close();
	}

	public List<DBObject> updateFlagToComplete(String collectionName,List<DBObject> batchRows) {
		if(batchRows!=null && !batchRows.isEmpty()){
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				DBObject setValue = new BasicDBObject();
				setValue.put("flag", 1);
				for (DBObject row : batchRows) {
					row.put(Const.FIELD_FLAG, Const.STATUS_COMPLETED);
					collection.save(row);
				}
			}
		} else{
			logger.error("batchRows is empty");
		}
		return batchRows;
	}
	
	public List<DBObject> updateFlagToNotComplete(String collectionName,List<DBObject> batchRows,String errMsg) {
		if(batchRows!=null && !batchRows.isEmpty()){
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				DBObject setValue = new BasicDBObject();
				setValue.put("flag", 1);
				for (DBObject row : batchRows) {
					row.put(Const.FIELD_FLAG, Const.STATUS_NOT_COMPLETED);
					row.put(Const.FIELD_ETL_ERRMSG, errMsg);
					collection.save(row);
				}
			}
		} else{
			logger.error("batchRows is empty");
		}
		return batchRows;
	}
	
	public List<DBObject> updateFlagToDraft(String collectionName,List<DBObject> batchRows) {
		if(batchRows!=null && !batchRows.isEmpty()){
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				DBObject setValue = new BasicDBObject();
				setValue.put("flag", 1);
				for (DBObject row : batchRows) {
					row.put(Const.FIELD_FLAG, Const.STATUS_DRAFT);
					collection.save(row);
				}
			}
		} else{
			logger.error("batchRows is empty");
		}
		return batchRows;
	}

	
	public ResultSetDTO fetchAndUpdateAvailableToRead(String previousCollectionName) {
		ResultSetDTO dto = new ResultSetDTO();
		boolean hasAvailabeDataInCollection = false;
		String collectionName = "";
		List<DBObject> availableRows = new ArrayList<>();
		if(COLLECTION_NAMES!=null){
			String[] collectionArr = COLLECTION_NAMES.split(SEPARATOR);
			if(collectionArr.length>0){
				collectionArr = Util.reorgenizeCollectionArr(previousCollectionName,collectionArr);
				db.requestStart();
				db.requestEnsureConnection();
				DBCursor cursor = null;
				for (int i = 0; i < collectionArr.length; i++) {
					collectionName = collectionArr[i];
					if(db.collectionExists(collectionName)){
						DBCollection collection = db.getCollection(collectionName);
						
//						.append("shardcollection", "track.PA").append("key",new BasicDBObject("tracking-id","1"))
//						DBObject cmd = new BasicDBObject();
//						cmd.put("enablesharding", "track");
//						cmd.put("shardcollection", "track.PA");
//						cmd.put("key", new BasicDBObject("tracking-id", 1));
////						CommandResult result = db.command(cmd);
//						db.command(cmd);
						
						logger.info("retrieve collection : "+collectionName);
						DBObject fields = null;
						DBObject update = new BasicDBObject("$set",new BasicDBObject(Const.FIELD_FLAG,Const.STATUS_IN_PROGRESS));
						boolean remove = false;
						boolean returnNew = true;
						boolean upsert = false;
						boolean multi = false;
						//using batchSize will retrieve all the records from each shard.
						//For example. if the fetchSize=2, shard number=3, then using batchSize will retrieve 6 records not 2.
//						cursor = collection.find(query).sort(sort).batchSize(fetchBatchSize);
						cursor = collection.find(query).sort(sort).limit(fetchBatchSize);
						while(cursor.hasNext()){
							DBObject row = cursor.next();
							logger.debug("retrieve row : "+row);
							ObjectId id = (ObjectId) row.get(Const.FIELD_ID);
							DBObject in = new BasicDBObject(Const.FIELD_ID,id).append(Const.FIELD_FLAG, new BasicDBObject("$ne",Const.STATUS_IN_PROGRESS));
//							WriteResult writeResult = collection.update(in, update, upsert, multi, WriteConcern.FSYNCED);
							WriteResult writeResult = collection.update(in, update);
							if(writeResult.isUpdateOfExisting()){
								availableRows.add(row);
								if(!hasAvailabeDataInCollection){
									hasAvailabeDataInCollection = true;
								}
							} else{
								logger.info("id : "+row.get("_id")+" can not update successfully, flag : "+row.get(Const.FIELD_FLAG));
								continue;
							}
						}
						/*for (int j = 0; j < FETCH_BATCH_SIZE; j++) {
							DBObject updatedRow = collection.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
							if(updatedRow!=null){
								availableRows.add(updatedRow);
								if(!hasAvailabeDataInCollection){
									hasAvailabeDataInCollection = true;
								}
							} else{
								logger.debug("no record fetch, exit...");
								break;
							}
						}*/
						if(hasAvailabeDataInCollection){
							break;
						}
					} else{
						logger.warn("collection : "+collectionName + " does not exist in db["+db.getName()+"], skip this collection...");
						continue;
					}
				}
				db.requestDone();
			} else{
				logger.error("collection ["+COLLECTION_NAMES+"] is empty!");
			}
		} else{
			logger.error(" COLLECTION_NAMES is empty!");
		}
		dto.setBatchRows(availableRows);
		dto.setNotEmpty(hasAvailabeDataInCollection);
		dto.setCollectionName(collectionName);
		return dto;
	}

	public List<DBObject> getMessages(String collectionName, List<ObjectId> rowIds) {
		if(rowIds!=null){
			logger.debug("query rows by the given row ids : "+Util.convertObjectIdListToStr(rowIds));
		} else{
			logger.warn("rowIds is empty");
		}
		List<DBObject> selectRows = new ArrayList<>();
		DBCursor cursor = null;
		DBObject in = new BasicDBObject(Const.FIELD_ID,new BasicDBObject("$in",rowIds)).append(Const.FIELD_FLAG, Const.STATUS_IN_PROGRESS);
		try {
			if(db.collectionExists(collectionName)){
				DBCollection collection = db.getCollection(collectionName);
				cursor = collection.find(in); 
				while(cursor.hasNext()){
					selectRows.add(cursor.next());
				}
				logger.debug(" select rowCount :"+cursor.count());
			} else{
				String msg = "collection:"+collectionName+" not found!";
				logger.error(msg);
				throw new Exception(msg);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(cursor!=null){
				cursor.close();
			}
		}
		return selectRows;
	}

	public ResultSetDTO fetchInprogressToRead(String previousCollectionName,int fetchBatchSize) {
		ResultSetDTO dto = new ResultSetDTO();
		boolean hasAvailabeDataInCollection = false;
		String collectionName = "";
		List<DBObject> availableRows = new ArrayList<>();
		DBObject longestRow = null;
		if(COLLECTION_NAMES!=null){
			String[] collectionArr = COLLECTION_NAMES.split(SEPARATOR);
			if(collectionArr.length>0){
				collectionArr = Util.reorgenizeCollectionArr(previousCollectionName,collectionArr);
//				db.requestStart();
//				db.requestEnsureConnection();
				DBCursor cursor = null;
				for (int i = 0; i < collectionArr.length; i++) {
					collectionName = collectionArr[i];
					if(db.collectionExists(collectionName)){
						DBCollection collection = db.getCollection(collectionName);
						logger.info("retrieve collection : "+collectionName);
						
						DBObject query = new BasicDBObject().append(Const.FIELD_FLAG, Const.STATUS_IN_PROGRESS);
						DBObject sort = new BasicDBObject(Const.FIELD_TIME,1);
						
						cursor = collection.find(query).sort(sort).limit(fetchBatchSize);
						while(cursor.hasNext()){
							DBObject row = cursor.next();
							availableRows.add(row);
							if(!hasAvailabeDataInCollection){
								hasAvailabeDataInCollection = true;
							}
							
							if(longestRow ==null || longestRow.keySet().size()<row.keySet().size()){
								longestRow = row;
							}
						}
						if(hasAvailabeDataInCollection){
							break;
						}
					} else{
						logger.warn("collection : "+collectionName + " does not exist in db["+db.getName()+"], skip this collection...");
						continue;
					}
				}
//				db.requestDone();
			} else{
				logger.error("collection ["+COLLECTION_NAMES+"] is empty!");
			}
		} else{
			logger.error(" COLLECTION_NAMES is empty!");
		}
		dto.setBatchRows(availableRows);
		dto.setNotEmpty(hasAvailabeDataInCollection);
		dto.setCollectionName(collectionName);
		dto.setLongestRow(longestRow);
		return dto;
	}

}
