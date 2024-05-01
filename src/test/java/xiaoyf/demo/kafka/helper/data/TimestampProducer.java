package xiaoyf.demo.kafka.helper.data;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static xiaoyf.demo.kafka.helper.Const.BOOTSTRAP_SERVER;
import static xiaoyf.demo.kafka.helper.Const.TIMESTAMP_INPUT_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.TIMESTAMP_LOG_APPEND_TIME_TOPIC;

public class TimestampProducer {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		long ts = ZonedDateTime.parse(
						"2020-02-20T00:00:04Z" ,
						DateTimeFormatter.ISO_DATE_TIME//.ofPattern ("yyyy-MM-ddTHH:mm:ssZ")
				)
				.toInstant()
				.toEpochMilli();
		ProducerRecord<String, String> createTimeRecord = new ProducerRecord<>(TIMESTAMP_INPUT_TOPIC, 0, ts, "K4", "V4");
		ProducerRecord<String, String> logAppendTimeRecord = new ProducerRecord<>(TIMESTAMP_LOG_APPEND_TIME_TOPIC, 0, ts, "K4", "V4");

		try {
			producer.send(createTimeRecord);
			producer.send(logAppendTimeRecord);
		} catch(SerializationException e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}
}

/*
 When producing to a normal topic (record timestamp defaulted to CreateTime), record timestamp is defined by producer.
 When producing to a topic set to LogAppendTime, record timestamp is NOT defined by producer
 */