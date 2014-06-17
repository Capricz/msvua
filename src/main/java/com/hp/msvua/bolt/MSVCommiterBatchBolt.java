package com.hp.msvua.bolt;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

import com.hp.msvua.dao.MongoManager;
import com.hp.msvua.dao.VerticaManager;
import com.hp.msvua.util.Const;
import com.mongodb.DBObject;

public class MSVCommiterBatchBolt extends BaseTransactionalBolt implements ICommitter {
	
	static Logger logger = Logger.getLogger(MSVCommiterBatchBolt.class);
	
	TransactionAttempt id;
	BatchOutputCollector collector;
	MongoManager mongoManager;
	VerticaManager verticaManager;
	
	List<DBObject> batchRows;
	String collectionName;
	DBObject longestRow;
	
	public MSVCommiterBatchBolt(){
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context,BatchOutputCollector collector, TransactionAttempt id) {
		logger.debug("in MSVCommiterBatchBolt - prepare");
		this.id = id;
		this.collector = collector;
		mongoManager = new MongoManager();
		verticaManager = new VerticaManager();
		logger.debug("in MSVCommiterBolt - instance batchRows...");
		batchRows = new ArrayList<>();
	}

	@Override
	public void execute(Tuple tuple) {
		logger.debug("in MSVCommiterBolt - execute");
		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		logger.debug("txid : "+tx.getTransactionId());
		String collection = (String) tuple.getValueByField("collection");
		logger.debug("collection : "+collection);
		DBObject row =  (DBObject) tuple.getValueByField("row");
		logger.debug("receive row: "+row);
		if(row!=null){
			Integer flag = (Integer) row.get(Const.FIELD_FLAG);
			logger.debug("row flag : "+flag);
			if(Const.STATUS_IN_PROGRESS == flag){
				collectionName = collection;
				logger.debug("adding row to batchRows");
				batchRows.add(row);
				if(longestRow==null || longestRow.keySet().size()<row.keySet().size()){
					longestRow = row;
				}
			} else{
				logger.debug(" row flag is invalid, flag : "+flag);
			}
		} else{
			logger.error("row is empty");
		}
	}

	@Override
	public void finishBatch() {
		if(batchRows==null || batchRows.isEmpty()){
			logger.error("batchRows is empty, couldn't insert row to vertica DB");
			return;
		} else{
			logger.debug("current batchRows size : "+batchRows.size());
		}
		String  rowId = "";
		try {
			logger.debug("add row to vertica batch");
			logger.debug("longestRow : "+longestRow);
			verticaManager.generatePreparedStatement(collectionName,longestRow);
			
			for (DBObject row : batchRows) {
				ObjectId oId = (ObjectId) row.get(Const.FIELD_ID);
				logger.debug("rowid : "+oId.toString());
				rowId = oId.toString();
				logger.debug("################prepare to adding row to batch :"+row);
				verticaManager.addRowToBatch(row,longestRow);
			}
			logger.debug("finish add row to batch..., prepare to execute batch rows..");
			verticaManager.executeBatchRows();
			mongoManager.updateFlagToComplete(collectionName,batchRows);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			mongoManager.updateFlagToNotComplete(collectionName,batchRows,"[errId:"+rowId+"]"+e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug(e.getMessage());
			mongoManager.updateFlagToNotComplete(collectionName,batchRows,"[rowId:"+rowId+"]"+e.getMessage());
		} finally{
			mongoManager.close();
			verticaManager.close();
		}
	}

	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
