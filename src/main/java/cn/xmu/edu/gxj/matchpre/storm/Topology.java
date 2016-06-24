package cn.xmu.edu.gxj.matchpre.storm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;


public class Topology {

	
	
	public static void main(String[] args){
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(ConStant.LOFTER_SPOUT, SpoutFactory.buildSpout(ConStant.LOFTER_TOPIC, ConStant.LOFTER_SPOUT));
//		builder.setSpout(Fields., spout)
		
		builder.setBolt(ConStant.LOGGER_BOLT, new LoggerBolt()).shuffleGrouping(ConStant.LOFTER_SPOUT);
		builder.setBolt(ConStant.HIST_BOLT, new HistBolt()).shuffleGrouping(ConStant.LOGGER_BOLT);
		builder.setBolt(ConStant.IMG_SIGN_BOLT, new ImageHashBolt()).shuffleGrouping(ConStant.HIST_BOLT);
		builder.setBolt(ConStant.OCR_BOLT, new OcrBolt()).shuffleGrouping(ConStant.IMG_SIGN_BOLT);
		builder.setBolt(ConStant.SAR_BOLT, new SarBolt()).shuffleGrouping(ConStant.OCR_BOLT);
		
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

