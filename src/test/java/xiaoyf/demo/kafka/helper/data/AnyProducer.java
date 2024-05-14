package xiaoyf.demo.kafka.helper.data;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;

import static xiaoyf.demo.kafka.helper.Const.BOOTSTRAP_SERVER;

public class AnyProducer {
	public static void produce(String topic, Object key, Object value) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
		props.put("schema.registry.url", "http://localhost:8081");
		KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

		ProducerRecord<Object, Object> order = new ProducerRecord<>(
				topic,
				key,
				value
			);

		try {
			producer.send(order);
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