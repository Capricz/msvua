package com.hp.msvua.spout;

import java.net.UnknownHostException;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalSpout;
import backtype.storm.tuple.Fields;


public class MSVTransactionalSpout extends BaseTransactionalSpout<TransactionMetadata> {
	
	static Logger logger = Logger.getLogger(MSVTransactionalSpout.class);
	
	public MSVTransactionalSpout(){
		logger.debug("in MSVTransactionalSpout - constructor");
	}

	@Override
	public backtype.storm.transactional.ITransactionalSpout.Coordinator<TransactionMetadata> getCoordinator(Map conf, TopologyContext context) {
		logger.debug("in MSVTransactionalSpout - initialize ITransactionalSpout.Coordinator");
		MSVTransactionalSpoutCoordinator coordinator = null;
		try {
			logger.debug("initializing MSVTransactionalSpoutCoordinator");
			coordinator = new MSVTransactionalSpoutCoordinator();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		if(coordinator==null){
			logger.error("coordinator is null");
			return null;
		}
		return coordinator;
	}

	@Override
	public backtype.storm.transactional.ITransactionalSpout.Emitter<TransactionMetadata> getEmitter(Map conf, TopologyContext context) {
		logger.debug("in MSVTransactionalSpout - initialize ITransactionalSpout.Emitter");
		return new MSVTransactionalSpoutEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		logger.debug("in MSVTransactionalSpout - initialize OutputFieldsDeclarer");
		declarer.declare(new Fields("txid","collection","row"));
	}

}
