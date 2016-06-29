package cn.xmu.edu.gxj.matchpre.storm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import cn.xmu.edu.gxj.matchpre.model.Reply;
import cn.xmu.edu.gxj.matchpre.service.MatchpService;
import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.JsonUtility;
import cn.xmu.edu.gxj.matchpre.util.MPException;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class OcrBolt extends BaseRichBolt {

	/*
	 * this bolt is to do image ocr.
	 */
	private Logger logger = LoggerFactory.getLogger(OcrBolt.class);
	private OutputCollector collector;
	private MatchpService service;

	@Override
	public void execute(Tuple arg0) {
		String json = arg0.getStringByField(ConStant.FIELD);
		try {
			byte[] bytes = JsonUtility.getAttributeasBinary(json, ConStant.IMAGE_BYTE);
			ByteArrayEntity entity = new ByteArrayEntity(bytes);
			String answer = service.responese(entity);
			json = JsonUtility.setAttribute(json, ConStant.OCR_FIELD, answer);
			logger.info("ocr length : {}", answer);
			collector.emit(new Values(json));
			collector.ack(arg0);
		} catch (MPException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			collector.fail(arg0);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
		service = new MatchpService("ocr");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields(ConStant.FIELD));
	}

	@Override
	public void cleanup() {
		try {
			service.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
