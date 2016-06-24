package cn.xmu.edu.gxj.matchpre.storm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.core.util.Constants;
import org.apache.storm.shade.org.yaml.snakeyaml.scanner.Constant;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import cn.xmu.edu.gxj.matchpre.model.Reply;
import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.JsonUtility;
import cn.xmu.edu.gxj.matchpre.util.MPException;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class HistBolt extends BaseRichBolt{

	/*
	 * this bolt is to add image histogram
	 */
	private Logger logger = LoggerFactory.getLogger(HistBolt.class);
	private CloseableHttpClient  httpclient;
	private String hist_url = "http://" + MatchpConfig.getMATCHP_SERVICE_IP() + "/hist/";
	private HttpPost post ;
	private OutputCollector collector;
	
	private final String hist = "hist";

	
	@Override
	public void execute(Tuple arg0) {
		String json = arg0.getStringByField(ConStant.FIELD);
		try {
			String img = JsonUtility.getAttributeasStr(json, ConStant.IMG_FIELD);
			String jsonText = JsonUtility.newJsonString("image", img);
			StringEntity params = new StringEntity(jsonText, "utf-8");
			post.setEntity(params);
			CloseableHttpResponse response = httpclient.execute(post);
			HttpEntity entity = response.getEntity();
            if (entity != null) {
            	String replyStr = EntityUtils.toString(entity,"UTF-8");
            	Reply reply = new Gson().fromJson(replyStr, Reply.class);
            	String arrayStr = "";
            	if (reply.getCode() == 200) {
					arrayStr = reply.getMessage();
					
					json = JsonUtility.setAttribute(json, hist, arrayStr.split(","));
					logger.info("image hist: {}" , arrayStr);
					collector.emit(new Values(json));
				} 
            }else{
            	throw new MPException(ErrCode.Invalid_IMG, img + " is valid.");
            } 
            response.close();
		} catch (MPException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (ClientProtocolException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		httpclient = HttpClients.createDefault();
		post = new HttpPost(hist_url);
		post.addHeader("content-type", "application/json");
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields(ConStant.FIELD));
	}

}
