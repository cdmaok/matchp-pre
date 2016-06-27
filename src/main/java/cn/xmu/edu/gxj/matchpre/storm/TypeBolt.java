package cn.xmu.edu.gxj.matchpre.storm;

import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.JsonUtility;
import cn.xmu.edu.gxj.matchpre.util.MPException;

public class TypeBolt extends BaseRichBolt{

	// this bolt is to get the type and calculate score
	private Logger logger = LoggerFactory.getLogger(TypeBolt.class);
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ConStant.FIELD));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {

		String json = (String) input.getValue(0);
		json = StringEscapeUtils.unescapeCsv(json);
		json = StringEscapeUtils.unescapeJson(json);
		String type = input.getSourceComponent();
		int socialScore = 0;
		String typeStr = "";
		try {
			switch (type) {
			case ConStant.LOFTER_SPOUT:
				socialScore = JsonUtility.getAttributeasInt(json, ConStant.LOFT_COMMENTS) + JsonUtility.getAttributeasInt(json, ConStant.LOFT_HOTS);
				typeStr = ConStant.LOFTER_TOPIC;
				break;
			case ConStant.WEIBO_SPOUT:
				socialScore = JsonUtility.getAttributeasInt(json, ConStant.WEIBO_COMMENTS) + JsonUtility.getAttributeasInt(json, ConStant.WEIBO_GOODS) + JsonUtility.getAttributeasInt(json, ConStant.WEIBO_REPOSTS);
				typeStr = ConStant.WEIBO_TOPIC;
				break;
			case ConStant.TUMBLR_SPOUT:
				socialScore = JsonUtility.getAttributeasInt(json, ConStant.TUMBLR_HOTS);
				typeStr = ConStant.TUMBLR_TOPIC;
				break;
			default:
				throw new MPException(ErrCode.Invalid_TEXT, "unknown type in calculating social score : " + type);
			}
			json = JsonUtility.setAttribute(json, ConStant.TYPE_FIELD, typeStr);
			json = JsonUtility.setAttribute(json, ConStant.SOSCORE_FIELD, socialScore);
			logger.info(json);
			collector.emit(new Values(json));
			collector.ack(input);
		} catch (MPException e) {
			e.printStackTrace();
			collector.fail(input);
		}
			
	}

}
