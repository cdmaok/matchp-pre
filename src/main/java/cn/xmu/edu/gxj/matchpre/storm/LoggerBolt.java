package cn.xmu.edu.gxj.matchpre.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerBolt extends BaseBasicBolt{

	private Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = (String) input.getValue(0);
		logger.info(json);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
