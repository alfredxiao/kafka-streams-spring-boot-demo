package xiaoyf.demo.kafka.processor;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import xiaoyf.demo.kafka.joiner.ClickLocationValueJoiner;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.APPLICATION_ID;
import static xiaoyf.demo.kafka.helper.Dumper.*;

@SpringBootTest(classes = {
        ClickProcessor.class,
        ClickLocationValueJoiner.class
})
@Import(ClickProcessorTest.ProcessorConfig.class)
@Tag("PartialIntegrationTest")
class ClickProcessorTest {

    @Autowired
    StreamsBuilder streamsBuilder;

    @Test
    void shouldEmitClickLocations() throws Exception {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final Topology topology = streamsBuilder.build(props);

        dumpTopology("clickProcessor", topology);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<String, String> clicks = testDriver
                    .createInputTopic("click",
                            new StringSerializer(),
                            new StringSerializer());
            final TestInputTopic<String, String> locations = testDriver
                    .createInputTopic("location",
                            new StringSerializer(),
                            new StringSerializer());
            final TestOutputTopic<String, String> clickLocations = testDriver
                    .createOutputTopic("click-location",
                            new StringDeserializer(),
                            new StringDeserializer());

                                                        locations.pipeInput("100", "Hong Kong");
                                                        locations.pipeInput("100", "Australia");
            clicks.pipeInput("100", "iPhone page");
                                                        locations.pipeInput("100", "China");
            clicks.pipeInput("100", "Jeep page");
            clicks.pipeInput("200", "Furniture page");
                                                        locations.pipeInput("200", "Europe");

            dumpTestDriverStats(testDriver);
            dumpTopicAndSchemaList();

            var output = clickLocations.readKeyValuesToList();
            assertThat(output).hasSize(2);
            assertThat(output.get(0)).isEqualTo(KeyValue.pair("100", "iPhone page from Australia"));
            assertThat(output.get(1)).isEqualTo(KeyValue.pair("100", "Jeep page from China"));
        }
    }

    @TestConfiguration
    static class ProcessorConfig {
        @Bean("defaultKafkaStreamsBuilder")
        StreamsBuilder streamsBuilder() {
            return new StreamsBuilder();
        }
    }
}
