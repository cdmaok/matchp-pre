package cn.xmu.edu.gxj.matchpre.storm;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringKeyValueScheme;
import org.apache.storm.kafka.ZkHosts;

import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class SpoutFactory {

	public static KafkaSpout buildSpout(String topic,String id){
		SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(MatchpConfig.getKAFKA_ZK_CON_STR()), topic, "/" + topic,id);
		spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		return new KafkaSpout(spoutConfig);
	}
}
