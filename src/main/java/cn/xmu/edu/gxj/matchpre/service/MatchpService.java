package cn.xmu.edu.gxj.matchpre.service;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import cn.xmu.edu.gxj.matchpre.model.Reply;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.MPException;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class MatchpService {
	
	private CloseableHttpClient httpClient;
	private HttpPost post;
	private String url;
	
	public MatchpService(String method) {
		url = "http://" + MatchpConfig.getMATCHP_SERVICE_IP() + "/" + method + "/";
		post = new HttpPost(url);
		httpClient = HttpClients.createDefault();
	}
	
	public String responese(HttpEntity entity) throws MPException{
		try {
			if (entity instanceof StringEntity) {
				post.addHeader("content-type", "application/json");
			}else if (entity instanceof ByteArrayEntity) {
				post.setHeader("Content-type", "application/octet-stream");
			}else {
				throw new MPException(ErrCode.Invalid_IMG, "unknown type for post " + post.getClass().getName());
			}
			post.setEntity(entity);
			CloseableHttpResponse response = httpClient.execute(post);
			HttpEntity resEntity = response.getEntity();
			if (null == resEntity) {
				throw new MPException(ErrCode.Invalid_IMG, "invalid request " + EntityUtils.toString(entity));
			}
			
        	String replyStr = EntityUtils.toString(resEntity,"UTF-8");
        	response.close();
        	Reply reply = new Gson().fromJson(replyStr, Reply.class);
        	if (reply.getCode() == 200) {
				return reply.getMessage();
			}else {
				throw new MPException(ErrCode.Invalid_IMG, "invalid request " + EntityUtils.toString(entity));
			} 
        	
		} catch (IOException | ParseException | JsonSyntaxException e) {
			e.printStackTrace();
			throw new MPException(ErrCode.Invalid_IMG, e.getMessage());
		} catch (MPException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void close() throws IOException{
		httpClient.close();
	}
}
