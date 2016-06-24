package cn.xmu.edu.gxj.matchpre.storm;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.xmu.edu.gxj.matchpre.util.ConStant;

public class LoggerBolt extends BaseBasicBolt{

	private Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = (String) input.getValue(0);
		json = StringEscapeUtils.unescapeCsv(json);
		json = StringEscapeUtils.unescapeJson(json);
		logger.info(json);
		collector.emit(new Values(json));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ConStant.FIELD));
	}

}
