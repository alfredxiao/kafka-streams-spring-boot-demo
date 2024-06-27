package xiaoyf.demo.kafka.helper.testhelper;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import xiaoyf.demo.kafka.helper.serde.AnyDeserializer;
import xiaoyf.demo.kafka.helper.serde.AnySerde;
import xiaoyf.demo.kafka.helper.serde.AnySerializer;

import java.time.Instant;
import java.util.Properties;

@Slf4j
public class TopologyTestHelper {

    @Getter
    private final TopologyTestDriver testDriver;

    @Getter
    private final Topology  topology;

    public TopologyTestHelper(StreamsBuilder builder) {
        this(builder, null);
    }

    public TopologyTestHelper(StreamsBuilder builder, Instant initialWallClockTime) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, AnySerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AnySerde.class);

        topology = builder.build(props);

        this.testDriver =
                initialWallClockTime == null
                        ? new TopologyTestDriver(topology, props)
                        : new TopologyTestDriver(topology, props, initialWallClockTime);
    }

    @SuppressWarnings("unchecked")
    public <K, V> TestInputTopic<K, V> inputTopic(String topic) {
        return (TestInputTopic<K, V>) testDriver.createInputTopic(topic, new AnySerializer(true), new AnySerializer(false));
    }

    @SuppressWarnings("unchecked")
    public <K, V> TestOutputTopic<K, V> outputTopic(String topic) {
        return (TestOutputTopic<K, V>) testDriver.createOutputTopic(topic, new AnyDeserializer(true), new AnyDeserializer(false));
    }

    public void close() {
        this.testDriver.close();
    }
}
