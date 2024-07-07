package proj.rxjava.fortune;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

public class FortuneKafkaConsumer {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Entrez le nom du topic");
			return;
		}

		String topicName = args[0];

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "0.0.0.0:55010");
		props.setProperty("group.id", "test-consumer-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("session.timeout.ms", "30000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
		
		Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();
		System.out.println("list of topics:" + listTopics.keySet());
	

		consumer.subscribe(Arrays.asList(topicName));
		
		System.out.println("Souscris au topic " + topicName);
		
		while (true) {
		      ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));
		      if (records != null) {
		    	  for (ConsumerRecord<Long, String> record : records) {
		    		  System.out.printf("offset=%d key=%d value=%s\n", 
		    				  record.offset(), record.key(), record.value());
		    		  
		    	  }
		      }
		}
	}

}
