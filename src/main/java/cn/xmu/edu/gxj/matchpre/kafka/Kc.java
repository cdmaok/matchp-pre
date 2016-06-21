package cn.xmu.edu.gxj.matchpre.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.serialization.
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Kc {

	public static void produce(){ Properties props = new Properties();
	 props.put("bootstrap.servers", "121.192.180.198:9092");
	 props.put("acks", "all");
	 props.put("retries", 0);
	 props.put("batch.size", 16384);
	 props.put("linger.ms", 1);
	 props.put("buffer.memory", 33554432);
	 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	 Producer<String, String> producer = new KafkaProducer<>(props);
	 for(int i = 0; i < 100; i++){
	     producer.send(new ProducerRecord<String, String>("matchp", Integer.toString(i), Integer.toString(i)));
	     producer.flush();
	 }

	 producer.close();
	 }
	
	public static void main(String[] args) {
		produce();
		consume();
	}
	
	public static void consume(){
	     Properties props = new Properties();
	     props.put("bootstrap.servers", "121.19.180.198:9092");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("matchp", "lofter"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         System.out.println(records.count());
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	     }
	}

}
