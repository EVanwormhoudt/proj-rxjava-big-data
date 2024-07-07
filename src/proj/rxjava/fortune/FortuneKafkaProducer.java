package proj.rxjava.fortune;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;

public class FortuneKafkaProducer {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Entrez l'addresse ip et le nom du topic");
			return;
		}
		
		String ipAddress = args[0];
		String topicName = args[1];
		
		Properties props = new Properties(); 
		props.put("bootstrap.servers", "0.0.0.0:55011"); 
		props.put("acks", "all"); 
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				 
		Producer<Long, String> producer = new KafkaProducer<Long, String>(props);
		
		Observable<FortuneData> fortuneObs = FortuneStream.from(ipAddress, 10000);
		
		Disposable disposable = fortuneObs
				.subscribeOn(Schedulers.io())
				.observeOn(Schedulers.io())
				.subscribe(fd -> {
						long time = fd.getDate().getTime();
						String text = fd.getText();
						ProducerRecord<Long, String> record = 
								new ProducerRecord<Long, String>(topicName, time, text);
						producer.send(record);
						System.out.println("message déposé:" + text);

					},
					ex -> System.out.println(ex)
				);
		
		System.out.println("Hit key to stop fortune client");
		try {
			System.in.read();
			disposable.dispose();
			producer.close();
			System.out.println("Fortune stream terminated");
		}
		catch(IOException e) {
			e.printStackTrace();
		}		

	}

}
