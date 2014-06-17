package com.hp.msvua.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import clojure.main;

import com.mongodb.DBObject;

public class MSVNormalizeBatchBolt implements IBasicBolt {
	
	static Logger logger = Logger.getLogger(MSVNormalizeBatchBolt.class);
	
	public MSVNormalizeBatchBolt(){
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("txid","collection","row"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		logger.debug("in MSVNormalizeBolt - getComponentConfiguration");
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		TransactionAttempt tx = (TransactionAttempt) tuple.getValueByField("txid");
		String collection = (String) tuple.getValueByField("collection");
		DBObject row = (DBObject) tuple.getValueByField("row");
		logger.debug("in MSVNormalizeBatchBolt - execute, receive row : "+row);
		collector.emit(new Values(tx,collection,row));
	}

	@Override
	public void cleanup() {
	}

}
