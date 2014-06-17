package com.hp.msvua.dao;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.msvua.model.ResultSetDTO;
import com.hp.msvua.util.Const;
import com.hp.msvua.util.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TransferManager {
	
	static Logger logger = Logger.getLogger(TransferManager.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoManager mongoManager = new MongoManager();
		VerticaManager verticaManager = new VerticaManager();
		
//		Mongo mongoClient = null;
//		DB db = null;
		int fetchBatchSize = 2;
//		Calendar cal = Calendar.getInstance();
//		cal.set(2014, 0, 1, 12, 22, 30);
//		String collectionName = "SyncUpdate";
		List<DBObject> fetchRows = new ArrayList<>();
		
		String previousCollectionName = "";
		String currCollectionName = "";
		
		try {
			ResultSetDTO dto = null;
			do{
				dto = mongoManager.fetchInprogressToRead(previousCollectionName, fetchBatchSize);
				previousCollectionName = dto.getCollectionName();
				
				if(dto!=null && dto.isNotEmpty()){
					mongoManager.updateFlagToDraft(dto.getCollectionName(), dto.getBatchRows());
//					for (Iterator<DBObject> it = dto.getBatchRows().iterator(); it.hasNext();) {
//						DBObject row = (DBObject) it.next();
						
//						if(needCreateSql){
//							verticaManager.generatePreparedStatement(dto.getCollectionName(), row);
//							needCreateSql = false;
//						}
//						
//						isExistInVertica = verticaManager.isExistedInVertica(dto.getCollectionName(),row);
//						if(isExistInVertica){
//							continue;
//						}
//						
//						verticaManager.addRowToBatch(row, dto.getLongestRow());
//						fetchRows.add(row);
//					}
					
//					int[] executedCount = verticaManager.executeBatchRows();
//					mongoManager.updateFlagToComplete(dto.getCollectionName(), dto.getBatchRows());
					Util.sleepForawhile(1000);
				} else{
					logger.info("finish update...");
					break;
				}
			} while(dto!=null && dto.isNotEmpty());
			
		} catch (Exception e) {
			e.printStackTrace();
			mongoManager.updateFlagToNotComplete(currCollectionName, fetchRows,e.getMessage());
		} finally{
			mongoManager.close();
			verticaManager.close();
		}
	}

}
