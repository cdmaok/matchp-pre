package cn.xmu.edu.gxj.matchpre.storm;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.print.DocFlavor.STRING;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import cn.xmu.edu.gxj.matchpre.model.Reply;
import cn.xmu.edu.gxj.matchpre.util.ConStant;
import cn.xmu.edu.gxj.matchpre.util.ErrCode;
import cn.xmu.edu.gxj.matchpre.util.JsonUtility;
import cn.xmu.edu.gxj.matchpre.util.MPException;
import cn.xmu.edu.gxj.matchpre.util.MatchpConfig;

public class ImgSizeBolt extends BaseRichBolt{

	/*
	 * this bolt is to get image's size
	 */
	private Logger logger = LoggerFactory.getLogger(ImgSizeBolt.class);
	private OutputCollector collector;
	

	
	@Override
	public void execute(Tuple arg0) {
		String json = arg0.getStringByField(ConStant.FIELD);
		try {
			String imgUrl = JsonUtility.getAttributeasStr(json, ConStant.IMG_FIELD);
			BufferedImage image = ImageIO.read(new URL(imgUrl));
			double height = image.getHeight();
			double width = image.getWidth();
			double size = height / width;
			json = JsonUtility.setAttribute(json, ConStant.SIZE_FIELD, size);
			
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			String[] splits = imgUrl.split("\\.");
			String format = splits[splits.length - 1];
			ImageIO.write(image, format, stream);
			byte[] bytes = stream.toByteArray();
			stream.close();
			json = JsonUtility.setAttribute(json, ConStant.IMAGE_BYTE, bytes);
			
			collector.emit(new Values(json));
			collector.ack(arg0);
		} catch (MPException | IOException  e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			collector.fail(arg0);
		}
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields(ConStant.FIELD));
	}
	
    @Override
    public void cleanup() {

    } 

}
