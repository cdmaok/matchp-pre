package cn.xmu.edu.gxj.matchpre.util;

import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatchpConfig {
	private static Logger logger = LoggerFactory.getLogger(MatchpConfig.class);
	
	private static String KAFKA_CON_STR ;
	private static String KAFKA_ZK_CON_STR;
	private static String STORM_ZK_SERVER;
	private static int STORM_ZK_PORT;
	private static String MATCHP_SERVICE_IP;
	private static String MATCHP_IP;

	
	static{
		String FileName = "/config.properties";
//		change to read from stream instead of reading file.
//		String FilePath = MatchpConfig.class.getResource(FileName).getPath();
//		logger.info("reading config file from {}",FilePath);

		Properties properties = new Properties();
		try {
//			properties.load(new FileInputStream(FilePath));
			boolean jar = isJar();
			
			properties.load(MatchpConfig.class.getResourceAsStream(FileName));
			
			KAFKA_CON_STR = getKey(properties, "KAFKA_CON_STR");
			KAFKA_ZK_CON_STR = getKey(properties, "ZK_CON_STR");
			STORM_ZK_SERVER = getKey(properties, "STORM_ZK_SERVER");
			STORM_ZK_PORT = Integer.parseInt(getKey(properties, "STORM_ZK_PORT"));
			MATCHP_SERVICE_IP = getKey(properties, "MATCHP_SERVICE_IP");
			MATCHP_IP = getKey(properties, "MATCHP_IP");
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
		
	}
	
	public static boolean isJar(){
		boolean flag = false;
		String cp = MatchpConfig.class.getResource("/config.properties").toString();
		if (cp.startsWith("jar")) {
			flag = true;
		}
		return flag;
	}
	
	public static String getKey(Properties prop,String key) throws MPException{
		if (prop == null || !prop.containsKey(key)) {
			throw new MPException("missing field " + key + " in file", ErrCode.Missing_Field);
		}
		return prop.getProperty(key);
	}
	
	public static void main(String[] args) {
//		MatchpConfig.
		System.out.println(MatchpConfig.getKAFKA_CON_STR());
		System.out.println(MatchpConfig.getKAFKA_ZK_CON_STR());
		System.out.println(MatchpConfig.getSTORM_ZK_SERVER());
	}

	public static String getKAFKA_CON_STR() {
		return KAFKA_CON_STR;
	}

	public static void setKAFKA_CON_STR(String kAFKA_CON_STR) {
		KAFKA_CON_STR = kAFKA_CON_STR;
	}

	public static String getKAFKA_ZK_CON_STR() {
		return KAFKA_ZK_CON_STR;
	}

	public static void setKAFKA_ZK_CON_STR(String kAFKA_ZK_CON_STR) {
		KAFKA_ZK_CON_STR = kAFKA_ZK_CON_STR;
	}

	public static String getSTORM_ZK_SERVER() {
		return STORM_ZK_SERVER;
	}

	public static void setSTORM_ZK_SERVER(String sTORM_ZK_SERVER) {
		STORM_ZK_SERVER = sTORM_ZK_SERVER;
	}

	public static int getSTORM_ZK_PORT() {
		return STORM_ZK_PORT;
	}

	public static void setSTORM_ZK_PORT(int sTORM_ZK_PORT) {
		STORM_ZK_PORT = sTORM_ZK_PORT;
	}

	public static String getMATCHP_SERVICE_IP() {
		return MATCHP_SERVICE_IP;
	}

	public static void setMATCHP_SERVICE_IP(String mATCHP_SERVICE_IP) {
		MATCHP_SERVICE_IP = mATCHP_SERVICE_IP;
	}

	public static String getMATCHP_IP() {
		return MATCHP_IP;
	}

	public static void setMATCHP_IP(String mATCHP_IP) {
		MATCHP_IP = mATCHP_IP;
	}
	
}
