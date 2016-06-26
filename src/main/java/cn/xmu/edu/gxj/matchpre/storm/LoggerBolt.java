package cn.xmu.edu.gxj.matchpre.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.xmu.edu.gxj.matchpre.util.ConStant;

public class LoggerBolt extends BaseBasicBolt{

	private Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String string = input.getStringByField(ConStant.FIELD);
		logger.info(string);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ConStant.FIELD));
	}

}
