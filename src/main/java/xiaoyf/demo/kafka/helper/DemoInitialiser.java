package xiaoyf.demo.kafka.helper;

import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.math.BigDecimal;
import java.util.Properties;

import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Fixtures.customerDetail;
import static xiaoyf.demo.kafka.helper.Fixtures.customerOrder;

public class DemoInitialiser {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("auto.register.schemas", true);

        KafkaProducer<CustomerOrderKey, CustomerOrder> orderProducer = new KafkaProducer<>(props);
        KafkaProducer<CustomerDetailsKey, CustomerDetails> detailProducer = new KafkaProducer<>(props);

        long timestamp = System.currentTimeMillis();

        var order = customerOrder(200015, 2000, "ABC", new BigDecimal("10000"), "Camp1");

        var details = customerDetail(2000, "Alfred", "alfred@gmail.com", "Camp1");

        ProducerRecord<CustomerOrderKey, CustomerOrder> record1 = new ProducerRecord<>(CUSTOMER_ORDER_TOPIC, order.key(), order.value());
        ProducerRecord<CustomerDetailsKey, CustomerDetails> record2 = new ProducerRecord<>(CUSTOMER_DETAIL_TOPIC, details.key(), details.value());
        try {
            orderProducer.send(record1);
//            detailProducer.send(record2);
        } catch(SerializationException e) {
            e.printStackTrace();
        } finally {
            orderProducer.flush();
            orderProducer.close();
            detailProducer.flush();
            detailProducer.close();
        }

    }
}
