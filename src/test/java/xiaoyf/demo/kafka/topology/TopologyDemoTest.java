package xiaoyf.demo.kafka.topology;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.PRIMARY_APPLICATION_ID;
import static xiaoyf.demo.kafka.helper.debug.Dumper.dumpTestDriverStats;
import static xiaoyf.demo.kafka.helper.debug.Dumper.dumpTopology;

/**
 * TopologyTest demonstrates unit testing topologies with tables in it.
 */
@Slf4j
public class TopologyDemoTest {

    @Test
    public void table() {
        var props = defaultProperties();

        Topology topology1 = tableFilterTopology(props);
        Topology topology2 = toTableFilterTopology(props);

        log.info(topology1.describe().toString());
        log.info(topology2.describe().toString());

        log.info("tableTopology equals toTableTopology? {}", topology1.equals(topology2));
    }

    @Test
    public void shouldTableAndFilter() {
        var props = defaultProperties();
        Topology topology = tableFilterTopology(props);

        dumpTopology("table and filter", topology);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> topic1 = testDriver
                    .createInputTopic("topic1", new StringSerializer(), new StringSerializer());
            final TestOutputTopic<String, String> topic2 = testDriver
                    .createOutputTopic("topic2", new StringDeserializer(), new StringDeserializer());

            topic1.pipeInput("alfred", "blue");
            topic1.pipeInput("brian", "red");
            topic1.pipeInput("alfred", "red");
            topic1.pipeInput("brian", "green");

            dumpTestDriverStats(testDriver);

            var output = topic2.readKeyValuesToList();
            assertThat(output).hasSize(4);
            assertThat(output.get(3)).isEqualTo(KeyValue.pair("brian", "green"));
            assertThat(output.get(2)).isEqualTo(KeyValue.pair("alfred", "red"));
        }
    }

    @Test
    public void shouldJoinTablesWithExplicitMaterialization() {
        Topology topology = tableTableJoinTopologyExplicitMaterialization();
        dumpTopology("table table join", topology);

        verify(
                topology,
                List.of(
                        input("topic1", "alfred", "blue"),
                        input("topic1", "brian", "red"),
                        input("topic2", "alfred", "book"),
                        input("topic2", "brian", "bike")
                ),
                List.of(
                        output("topic3", "alfred", "blue/book"),
                        output("topic3", "brian", "red/bike")
                )
        );
    }

    @Test
    public void shouldJoinTablesWithImplicitMaterialization() {
        Topology topology = tableTableJoinTopologyImplicitMaterialization();
        dumpTopology("table table join", topology);

        verify(
                topology,
                List.of(
                        input("topic1", "alfred", "blue"),
                        input("topic1", "brian", "red"),
                        input("topic2", "alfred", "book"),
                        input("topic2", "brian", "bike")
                ),
                List.of(
                        output("topic3", "alfred", "blue/book"),
                        output("topic3", "brian", "red/bike")
                )
        );
    }

    private Record input(String topic, String key, String value) {
        return new Record(topic, key, value);
    }

    private Record output(String topic, String key, String value) {
        return new Record(topic, key, value);
    }

    private void verify(final Topology topology, final List<Record> inputs, final List<Record> outputs) {
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, defaultProperties())) {
            Map<String, TestInputTopic<String, String>> inputTopicMap = createTestInputTopics(inputs, testDriver);
            Map<String, TestOutputTopic<String, String>> outputTopicMap = createTestOutputTopics(outputs, testDriver);

            inputs.forEach(input -> {
                inputTopicMap.get(input.topic).pipeInput(input.key, input.value);
            });

            dumpTestDriverStats(testDriver);

            // assume there is always one and only one output topic
            String outputTopicName = outputs.get(0).topic;
            TestOutputTopic<String, String> outputTopic = outputTopicMap.get(outputTopicName);

            var actualOutputs = outputTopic.readKeyValuesToList();
            assertThat(actualOutputs).hasSize(outputs.size());

            for (int i=0; i < outputs.size(); i++) {
                var output = outputs.get(i);
                var actualOutput = actualOutputs.get(i);
                assertThat(actualOutput.key).isEqualTo(output.key);
                assertThat(actualOutput.value).isEqualTo(output.value);
            }
        }
    }

    private Map<String, TestOutputTopic<String, String>> createTestOutputTopics(List<Record> outputs, TopologyTestDriver testDriver) {
        Set<String> outputTopics = outputs
                .stream()
                .map(r -> r.topic)
                .collect(Collectors.toSet());

        Map<String, TestOutputTopic<String, String>> outputTopicMap = new HashMap<>();
        outputTopics.forEach(topic -> {
            final TestOutputTopic<String, String> outputTopic = testDriver
                    .createOutputTopic(topic, new StringDeserializer(), new StringDeserializer());
            outputTopicMap.put(topic, outputTopic);
        });

        return outputTopicMap;
    }

    private Map<String, TestInputTopic<String, String>> createTestInputTopics(List<Record> inputs, TopologyTestDriver testDriver) {
        Set<String> inputTopics = inputs
                .stream()
                .map(r -> r.topic)
                .collect(Collectors.toSet());

        Map<String, TestInputTopic<String, String>> inputTopicMap = new HashMap<>();
        inputTopics.forEach(topic -> {
            final TestInputTopic<String, String> inputTopic = testDriver
                    .createInputTopic(topic, new StringSerializer(), new StringSerializer());
            inputTopicMap.put(topic, inputTopic);
        });

        return inputTopicMap;
    }

    private Properties defaultProperties() {
        final Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, PRIMARY_APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        return props;

    }

    private Topology toTableFilterTopology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream("topic1")
                .toTable()
                .filter((k, v) -> v != null)
                .toStream()
                .to("topic2");

        return builder.build(props);
    }

    private Topology tableFilterTopology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .table("topic1")
                .filter((k, v) -> v != null)
                .toStream()
                .to("topic2");

        return builder.build(props);
    }

    private Topology tableTableJoinTopologyImplicitMaterialization() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> table2 = builder.table("topic2");

        builder
                .<String, String>table("topic1")
                .join(
                        table2,
                        (v1, v2) -> v1 + "/" + v2)
                .toStream()
                .to("topic3");

        return builder.build(defaultProperties());
    }

    private Topology tableTableJoinTopologyExplicitMaterialization() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> table2 =
                builder.table(
                        "topic2",
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("store2").withLoggingDisabled());
        builder
                .<String, String>table(
                        "topic1",
                        Materialized.as("store1")
                )
                .join(
                        table2,
                        (v1, v2) -> v1 + "/" + v2,
                        Materialized.as("joined-store"))
                .toStream()
                .to("topic3");

        return builder.build(defaultProperties());
    }

    static class Record {
        String topic;
        String key;
        String value;

        public Record(String topic, String key, String value) {
            this.topic = topic;
            this.key = key;
            this.value = value;
        }
    }
}
