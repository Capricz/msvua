package com.hp.msvua;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.utils.Utils;

import com.hp.msvua.bolt.MSVCommiterBatchBolt;
import com.hp.msvua.bolt.MSVNormalizeBatchBolt;
import com.hp.msvua.spout.MSVTransactionalSpout;

public class MSVTransTopology {
	
	static Logger logger = Logger.getLogger(MSVTransTopology.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger.info("start logging...");
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("test", "spout", new MSVTransactionalSpout());
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("spout", new MSVReadSpout());
		builder.setBolt("normaize-data", new MSVNormalizeBatchBolt(), 5).shuffleGrouping("spout");
		builder.setBolt("vertica-committer", new MSVCommiterBatchBolt()).shuffleGrouping("normaize-data");
//		builder.setBolt("vertica-write", new MSVWriteBolt()).shuffleGrouping("normaize-data");
		
		Config config = new Config();
		
		boolean isDebug = true;
		if(args.length>0&&"start".equals(args[0])){
			isDebug = false;
			logger.debug("now on production...");
			System.out.println("now on production...");
		}
		config.setDebug(isDebug);
		logger.info("isDebug : "+isDebug);
		logger.info("create config...");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.buildTopology());
		
		if(isDebug){
			long idleTime = 60 * 60 * 1000;
			logger.info("prepare to sleeping... "+idleTime+"ms");
			Utils.sleep(idleTime);
			logger.info("prepare to shutdown...");
			cluster.shutdown();
		}
		
		/*int parallelism_hint = 1;
		builder.setBolt("write-vertica",new MSVWriteBatchBolt(), 4).shuffleGrouping("normaize-data");
		config.setNumWorkers(5);
//		cluster.killTopology("dataTransfer");
//		Utils.sleep(1000 * 5);
		*/
	}

}
