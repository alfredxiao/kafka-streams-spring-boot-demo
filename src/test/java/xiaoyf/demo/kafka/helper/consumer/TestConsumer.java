package xiaoyf.demo.kafka.helper.consumer;


import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
public class TestConsumer<K, V> {
    public static final Duration SLICE = Duration.ofMillis(200);
    private final int port;
    private final String topic;

    private final List<ConsumerRecord<K, V>> receivedRecords;
    private final String keyDeserializerClassName;
    private final String valueDeserializerClassName;

    public TestConsumer(
            final int port,
            final String topic,
            final String keyDeserializerClassName,
            final String valueDeserializerClassName
            ) {
        this.port = port;
        this.topic = topic;
        this.keyDeserializerClassName = keyDeserializerClassName;
        this.valueDeserializerClassName = valueDeserializerClassName;

        this.receivedRecords = Collections.synchronizedList(new ArrayList<>());
    }

    public List<ConsumerRecord<K, V>> getReceivedRecords() {
        return this.receivedRecords;
    }

    private Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + this.port);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-"+UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return props;
    }

    private final class TestListener implements Runnable {
        private final Duration atLeast;
        private final Duration atMost;
        private final int stopAtCount;

        private TestListener(final Duration atLeast, final Duration atMost, final int stopAtCount) {
            this.atLeast = atLeast;
            this.atMost = atMost;
            this.stopAtCount = stopAtCount;
        }

        private boolean hasBeenListeningForLongEnough(long timeElapsed) {
            // not yet long up to 'timeElapsed'
            if (atLeast != null && timeElapsed < atLeast.toMillis()) {
                return false;
            }

            // have received enough records so we stop
            if (stopAtCount > 0 && receivedRecords.size() >= stopAtCount) {
                return true;
            }

            // exit if 'timeElapsed' is long enough
            return atMost != null && timeElapsed >= atMost.toMillis();
        }

        @Override
        public void run() {
            try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties())) {
                consumer.subscribe(Collections.singletonList(topic));

                long timeElapsed = 0;

                do {
                    ConsumerRecords<K, V> records = consumer.poll(SLICE);

                    for (ConsumerRecord<K, V> record : records) {
                        receivedRecords.add(record);
                    }

                    timeElapsed += SLICE.toMillis();

                } while (!hasBeenListeningForLongEnough(timeElapsed));
            } catch(Exception e) {
                log.error("Error while running TestConsumer", e);
            }
        }
    }

    public TestConsumer<K, V> startListening(final Duration atLeast, final Duration atMost, final int stopAtCount) {
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(new TestListener(atLeast, atMost, stopAtCount));

        return this;
    }
}
