package cn.xmu.edu.gxj.matchpre.storm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;


public class Topology {

	
	private static Logger logger = LoggerFactory.getLogger(Topology.class);
	
	public static void main(String[] args){
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(ConStant.LOFTER_SPOUT, SpoutFactory.buildSpout(ConStant.LOFTER_TOPIC, ConStant.LOFTER_SPOUT));
		builder.setSpout(ConStant.WEIBO_SPOUT, SpoutFactory.buildSpout(ConStant.WEIBO_TOPIC, ConStant.WEIBO_SPOUT));
		builder.setSpout(ConStant.TUMBLR_SPOUT, SpoutFactory.buildSpout(ConStant.TUMBLR_TOPIC, ConStant.TUMBLR_SPOUT));
		
		builder.setBolt(ConStant.TYPE_BOLT, new TypeBolt()).shuffleGrouping(ConStant.TUMBLR_SPOUT)
			.shuffleGrouping(ConStant.WEIBO_SPOUT).shuffleGrouping(ConStant.LOFTER_SPOUT);
		
		builder.setBolt(ConStant.IMG_SIZE_BOLT, new ImgSizeBolt(),5).shuffleGrouping(ConStant.TYPE_BOLT);
		
		builder.setBolt(ConStant.HIST_BOLT, new HistBolt(),5).shuffleGrouping(ConStant.IMG_SIZE_BOLT);
		builder.setBolt(ConStant.IMG_SIGN_BOLT, new ImgHashBolt()).shuffleGrouping(ConStant.HIST_BOLT);
		builder.setBolt(ConStant.OCR_BOLT, new OcrBolt(),10).shuffleGrouping(ConStant.IMG_SIGN_BOLT);
		builder.setBolt(ConStant.SAR_BOLT, new SarBolt(),3).shuffleGrouping(ConStant.OCR_BOLT);
		builder.setBolt(ConStant.SEN_BOLT, new SenBolt()).shuffleGrouping(ConStant.SAR_BOLT);

		builder.setBolt(ConStant.LOGGER_BOLT, new LoggerBolt()).shuffleGrouping(ConStant.SEN_BOLT);
		builder.setBolt(ConStant.INDEX_BOLT, new IndexBolt()).shuffleGrouping(ConStant.LOGGER_BOLT);
		
		
		StormTopology topology = builder.createTopology();
		
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(MatchpConfig.getSTORM_ZK_SERVER().split(",")));
		config.put(Config.STORM_ZOOKEEPER_PORT, MatchpConfig.getSTORM_ZK_PORT());
		config.setMessageTimeoutSecs(30);
		if (args.length != 1) {
			logger.error(" need a parameter local model(0) or cluster model(1)");
			return;
		}else {
			int mode = Integer.parseInt(args[0]);
			logger.info("mode is {}", mode);
			switch (mode) {
			case 0:
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("matchp", config, topology);
				break;
			case 1:
				 try {
					StormSubmitter.submitTopology("matchp", config, topology);
				} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
				break;
			default:
				logger.error("unknown mode {}",mode);
				return;
			}
		}

	}
	
}

