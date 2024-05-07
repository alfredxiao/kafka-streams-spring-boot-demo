package xiaoyf.demo.kafka.helper.testhelper;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class TopologyTestHelper {
    public static final Map<String, String> MOCK_CONFIG = Map.of(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy"
    );

    @Getter
    private final TopologyTestDriver testDriver;

    public TopologyTestHelper(StreamsBuilder builder) {
        this(builder, null);
    }

    public TopologyTestHelper(StreamsBuilder builder, Instant initialWallClockTime) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        Topology topology = builder.build(props);

        this.testDriver =
                initialWallClockTime == null
                        ? new TopologyTestDriver(topology, props)
                        : new TopologyTestDriver(topology, props, initialWallClockTime);
    }

    public <K extends SpecificRecord, V extends SpecificRecord> TestInputTopic<K, V> inputTopic(String topic) {
        return testDriver.createInputTopic(topic,
                this.<K>keySerde().serializer(),
                this.<V>valueSerde().serializer());
    }

    public <K extends SpecificRecord, V extends SpecificRecord> TestOutputTopic<K, V> outputTopic(String topic) {
        return testDriver.createOutputTopic(topic,
                this.<K>keySerde().deserializer(),
                this.<V>valueSerde().deserializer());
    }

    public <K extends SpecificRecord> Serde<K> keySerde() {
        Serde<K> keySerde = new SpecificAvroSerde<>();
        keySerde.configure(MOCK_CONFIG, true);

        return keySerde;
    }

    public <K extends SpecificRecord> Serde<K> valueSerde() {
        Serde<K> keySerde = new SpecificAvroSerde<>();
        keySerde.configure(MOCK_CONFIG, false);

        return keySerde;
    }

    public void close() {
        this.testDriver.close();
    }
}
