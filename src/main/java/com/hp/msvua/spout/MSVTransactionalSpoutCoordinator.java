package com.hp.msvua.spout;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import backtype.storm.transactional.ITransactionalSpout;

import com.hp.msvua.dao.MongoManager;
import com.hp.msvua.model.ResultSetDTO;
import com.hp.msvua.util.Configuration;
import com.hp.msvua.util.Const;
import com.hp.msvua.util.Util;
import com.mongodb.DBObject;

public class MSVTransactionalSpoutCoordinator implements ITransactionalSpout.Coordinator<TransactionMetadata> {
	
	static Logger logger = Logger.getLogger(MSVTransactionalSpoutCoordinator.class);
	
	MongoManager mongoManager;
	List<DBObject> nextBatchRows;
	String collectionName;
	String previousCollectionName;
	
	public MSVTransactionalSpoutCoordinator() throws UnknownHostException{
		logger.debug("in MSVTransactionalSpoutCoordinator - prepare to initialize mongo");
		mongoManager = new MongoManager();
		nextBatchRows = new ArrayList<>();
	}
	
	@Override
	public boolean isReady() {
		Util.sleepForawhile(Configuration.getValue(Const.STORM_COORDINATOR_INTERVAL));
		
		ResultSetDTO dto = mongoManager.fetchAndUpdateAvailableToRead(previousCollectionName);
		logger.info("ready to start a new transaction? : ["+(dto.isNotEmpty()?"Yes":"No")+"], fetch size : "+dto.getBatchRowsSize());
		if(dto.isNotEmpty()){
			nextBatchRows = dto.getBatchRows();
			collectionName = dto.getCollectionName();
			previousCollectionName = collectionName;
//			logger.debug("received data, prepare to sleep for a while...");
			return true;
		} else{ //sleep while there is no data found
			logger.debug("no data found, sleeping...");
			Util.sleepForawhile(Configuration.getValue(Const.STORM_COORDINATOR_NOT_FOUND_INTERVAL));
			return false;
		}
	}

	@Override
	public TransactionMetadata initializeTransaction(BigInteger txid,TransactionMetadata prevMetadata) {
		TransactionMetadata ret = initializeTransactionMetadat();
		if(nextBatchRows!=null && !nextBatchRows.isEmpty()){
			List<ObjectId> rowIds = Util.convertToIdList(nextBatchRows);
			ret = new TransactionMetadata(collectionName,rowIds);
			ObjectId[] objectIds = Util.convertObjectIdListToArr(rowIds);
			String objIdStr = Util.convertObjectIdArrToStr(objectIds);
			logger.debug("store transactionMetadata,txid:"+txid+" [collection:"+collectionName+"]objIds : "+objIdStr);
		}
		return ret;
	}

	//should not go through
	private TransactionMetadata initializeTransactionMetadat() {
		List<ObjectId> rowIds = new ArrayList<>();
		rowIds.add(new ObjectId("000000000000000000000001"));
		return new TransactionMetadata("PA",rowIds);
	}

	@Override
	public void close() {
		mongoManager.close();
	}


}
