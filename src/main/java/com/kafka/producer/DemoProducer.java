package com.kafka.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.myflink.taxipojo.TaxiPojo;


public class DemoProducer {

	public static void main(String[] args) throws InterruptedException {
		String topicName = "advtaxi";
		

		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");

		props.put("acks", "all");

		props.put("retries", 0);

		props.put("batch.size", 16384);

		props.put("linger.ms", 1);

		props.put("buffer.memory", 33554432);

		props.put("key.serializer", StringSerializer.class.getName());

		props.put("value.serializer", StringSerializer.class.getName());

		Random random =new Random();
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		String [] src = {"pune","nasik","delhi"};
		String [] dest = {"mumbai","nagpur","noida"};
		String [] driverName = {"Peter","Andrew","Paul"};
		ProducerRecord<String, String> lateRecord = null ;
		int counter=0;
		//long pickUpTime=1549893277394L;
		long pickUpTime  = System.currentTimeMillis();
		for (int i = 0; i < 2000; i++) {
			counter++;
			if(counter==5) {
				String l = new String(System.currentTimeMillis()+","+src[random.nextInt(1)] +"," +dest[random.nextInt(1)]);
				 lateRecord = 
						 		new ProducerRecord<String, String>(topicName,  l);
				 System.out.println("Late Record -> "+l);
				 
				// Thread.currentThread().sleep(10000);
				
			}
		
			if(counter==-12) {
				producer.send(lateRecord);
			}
			pickUpTime = pickUpTime+300000;
			
			//System.out.println("TimeStamp :::::"+System.currentTimeMillis());
			TaxiPojo taxi = new TaxiPojo(driverName[random.nextInt(3)], src[random.nextInt(2)],pickUpTime , dest[random.nextInt(2)], (pickUpTime+2000), (pickUpTime+30000),(random.nextInt(20)+100));
			producer.send(new ProducerRecord<String, String>(topicName, taxi.toString()));
	//	Thread.currentThread().sleep(200L);
		}
		producer.flush();
		producer.close();
	}
}
