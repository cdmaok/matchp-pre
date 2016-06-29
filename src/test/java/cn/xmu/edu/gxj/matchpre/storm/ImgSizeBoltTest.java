package cn.xmu.edu.gxj.matchpre.storm;

import static org.junit.Assert.*;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.imageio.ImageIO;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Ignore;
import org.junit.Test;

public class ImgSizeBoltTest {

	@Ignore
	@Test
	public void test() {
		String imgUrl = "http://ww2.sinaimg.cn/mw690/a1ab8e59jw1f59p38thd0j20c83qv7va.jpg";
		BufferedImage image;
		try {
			image = ImageIO.read(new URL(imgUrl));
			double height = image.getHeight();
			double width = image.getWidth();
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			String[] splits = imgUrl.split("\\.");
			String format = splits[splits.length - 1];
			ImageIO.write(image, format, stream);
			byte[] bytes = stream.toByteArray();
			stream.close();
			String img = Base64.encodeBase64String(bytes);
			byte[] back = Base64.decodeBase64(img);
			ByteArrayInputStream in = new ByteArrayInputStream(back);
			BufferedImage imageback = ImageIO.read(in);
			int heightback = imageback.getHeight();
			int widthback = imageback.getWidth();
			assertEquals(heightback, height,0);
			assertEquals(widthback, width,0);
			CloseableHttpClient httpClients = HttpClients.createDefault();
			HttpPost post = new HttpPost("http://121.192.180.198:9001/hist/");
			post.setHeader("Content-type", "application/octet-stream");
			post.setEntity(new ByteArrayEntity(bytes));
			HttpResponse response = httpClients.execute(post);
			HttpEntity entity = response.getEntity();
			System.out.println(EntityUtils.toString(entity, "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	@Test
	public void testpost(){
		try {

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
