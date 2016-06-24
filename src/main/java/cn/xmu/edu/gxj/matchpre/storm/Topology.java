package cn.xmu.edu.gxj.matchpre.storm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import cn.xmu.edu.gxj.matchpre.util.Fields;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;


public class Topology {

	
	
	public static void main(String[] args){
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(Fields.LOFTER_SPOUT, SpoutFactory.buildSpout(Fields.LOFTER_TOPIC, Fields.LOFTER_SPOUT));
//		builder.setSpout(Fields., spout)
		
		builder.setBolt(Fields.LOGGER_BOLT, new LoggerBolt()).shuffleGrouping(Fields.LOFTER_SPOUT);
		
		System.out.println("done");
		
		StormTopology topology = builder.createTopology();
		
		Config config = new Config();
		config.setDebug(true);
//		config.put(Config.TOPOLOGY_DEBUG, true);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(MatchpConfig.getSTORM_ZK_SERVER().split(",")));
		config.put(Config.STORM_ZOOKEEPER_PORT, MatchpConfig.getSTORM_ZK_PORT());
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("test", config, topology);
	}
	
}

