package cn.xmu.edu.gxj.matchpre.storm;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import cn.xmu.edu.gxj.matchpre.model.Reply;
import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.MPException;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class IndexBolt extends BaseRichBolt{
	
	private CloseableHttpClient client;
	private Logger logger = LoggerFactory.getLogger(IndexBolt.class);
	private String url = "http://" + MatchpConfig.getMATCHP_IP();
	private HttpPost post;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		client = HttpClients.createDefault();
		this.collector = collector;
		post = new HttpPost(url);
		post.addHeader("content-type", "application/json");
	}

	@Override
	public void execute(Tuple input) {
		String json = input.getStringByField(ConStant.FIELD);
		StringEntity params = new StringEntity(json, "UTF-8");
		post.setEntity(params);
		try {
			CloseableHttpResponse response = client.execute(post);
			HttpEntity entity = response.getEntity();
			if (entity == null) {
				throw new MPException(ErrCode.Index_FAIL, json + " index failed somehow");
			}
			String replyStr = EntityUtils.toString(entity,"UTF-8");
			Reply reply = new Gson().fromJson(replyStr, Reply.class);
			if (reply.getCode() != ErrCode.Index_Success) {
				throw new MPException(reply.getCode(), reply.getMessage());
			}
			collector.ack(input);
		} catch (MPException | JsonSyntaxException | IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			collector.fail(input);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ConStant.FIELD));
	}
    @Override
    public void cleanup() {
    	try {
    		client.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
    }

}
