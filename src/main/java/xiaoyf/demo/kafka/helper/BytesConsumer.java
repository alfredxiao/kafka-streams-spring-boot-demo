package xiaoyf.demo.kafka.helper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySchema;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionResponseWrapperSerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.SubscriptionWrapperSerde;

import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;

public class BytesConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g3");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class);


        try(final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(List.of(
                    CUSTOMER_ORDER_TOPIC,
                    CUSTOMER_DETAIL_TOPIC,
                    "kafka-demo-ORDER-JOINS-CUST-DETAIL-subscription-registration-topic",
                    "kafka-demo-ORDER-JOINS-CUST-DETAIL-subscription-response-topic",
                    "kafka-demo-ORDER-JOINS-CUST-DETAIL-subscription-store-changelog",
                    "kafka-demo-ORDER-JOINS-CUST-DETAIL-STORE-changelog",
                    PREMIUM_ORDER_TOPIC));

            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.of(10, ChronoUnit.MILLIS));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] key = record.key();
                    byte[] value = record.value();

                    String fromTopic = record.topic();

                    FileLogger.log("Topic: " + fromTopic);
                    FileLogger.log("Timestamp: " + record.timestamp());

                    KafkaAvroDeserializer keyDes = new KafkaAvroDeserializer();

                    Map<String, String> m = new HashMap<>();
                    m.put("schema.registry.url", "http://localhost:8081");
                    m.put("specific.avro.reader", "false");

                    keyDes.configure(m, true);
                    KafkaAvroDeserializer valueDes = new KafkaAvroDeserializer();
                    valueDes.configure(m, false);

                    var keyObj = des(keyDes, fromTopic, key);
                    var valueObj = des(valueDes, fromTopic, value);

                    FileLogger.log("key bytes: " + FileLogger.bytesString(key) + " -> " + keyObj);
                    FileLogger.log("value bytes: " + FileLogger.bytesString(value) + " -> " + valueObj);

                    Serde<GenericRecord> pkSerde = new GenericAvroSerde();
                    pkSerde.configure(m, false);
                    Serde<GenericRecord> fkSerde = new GenericAvroSerde();
                    fkSerde.configure(m, false);

                    Supplier<String> pkTopicSupplier = () -> fromTopic + "-pk";
                    Supplier<String> fkTopicSupplier = () -> fromTopic + "-fk";

                    if (fromTopic.endsWith("-subscription-registration-topic")) {
                        SubscriptionWrapperSerde<GenericRecord> subSerde = new SubscriptionWrapperSerde<>(pkTopicSupplier, pkSerde);
                        Object subObject = subSerde.deserializer().deserialize(fromTopic, value);
                        FileLogger.log("subObject:" + subObject);
                    }

                    if (fromTopic.endsWith("-subscription-response-topic")) {
                        SubscriptionResponseWrapperSerde<GenericRecord> respSerde = new SubscriptionResponseWrapperSerde<>(fkSerde);
                        Object respObject = respSerde.deserializer().deserialize(fromTopic, value);
                        FileLogger.log("respObject:" + respObject);
                    }

                    if (fromTopic.endsWith("-subscription-store-changelog")) {
                        CombinedKeySchema<GenericRecord, GenericRecord> ckSchema = new CombinedKeySchema<>(
                                fkTopicSupplier,
                                fkSerde,
                                pkTopicSupplier,
                                pkSerde
                        );
                        CombinedKey<GenericRecord, GenericRecord> ck = ckSchema.fromBytes(new Bytes(key));
                        FileLogger.log("substore key:" + ck);

                        SubscriptionWrapperSerde<GenericRecord> subSerde = new SubscriptionWrapperSerde<>(pkTopicSupplier, pkSerde);
                        Object subObject = subSerde.deserializer().deserialize(fromTopic, value);
                        FileLogger.log("substore value:" + subObject);
                    }
                }
                consumer.commitSync();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    static Object des(Deserializer<Object> deserializer, String topic, byte[] bytes) {
        try {
            return deserializer.deserialize(topic, bytes);
        } catch(Exception e) {
            // e.printStackTrace();
            return "CANNOT_DESERIALISE";
        }
    }
}
