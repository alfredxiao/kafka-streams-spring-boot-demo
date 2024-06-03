package xiaoyf.demo.kafka.topology.idshortener.registry;

import demo.model.OrderEnriched;
import demo.model.OrderKey;
import demo.model.OrderValue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.config.SharedTopologyConfiguration;
import xiaoyf.demo.kafka.helper.testhelper.TopologyTestHelper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        SharedTopologyConfiguration.class,
        ShortIdRegistryTopologyTest.TestConfig.class,
        ShortIdRegistryTopology.class,
        ShortIdRegistryTopologyConfiguration.class,
        ReverseKeyValueMapper.class,
        ShortIdRegistryProcessor.class,
    }
)
@TestPropertySource(
        properties = {"demo-streams.short-id-registry-app-enabled=true"}
)
public class ShortIdRegistryTopologyTest {
    final static String LONG_ID_TOPIC = "long-id";
    final static String LONG_ID_TO_SHORT_ID_TOPIC = "long-id-to-short-id";
    final static String SHORT_ID_TO_LONG_ID_TOPIC = "short-id-to-long-id";

    @Autowired
    @Qualifier("shortIdRegistryStreamsBuilder")
    private StreamsBuilder shortIdRegistryStreamsBuilder;

    private TestInputTopic<String, Integer> longIdTopic;
    private TestOutputTopic<String, Integer> longIdToShortIdTopic;
    private TestOutputTopic<Integer, String> shortIdToLongIdTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(shortIdRegistryStreamsBuilder);

        longIdTopic = helper.inputTopic(LONG_ID_TOPIC);
        longIdToShortIdTopic = helper.outputTopic(LONG_ID_TO_SHORT_ID_TOPIC);
        shortIdToLongIdTopic = helper.outputTopic(SHORT_ID_TO_LONG_ID_TOPIC);
    }

    @AfterEach
    void cleanup() {
        if (helper != null) {
            helper.close();
        };
    }

    @Test
    void shouldEmitLongIdToShortIdMapping() {
        longIdTopic.pipeInput("id001", -1);

        final List<KeyValue<String, Integer>> mapping = longIdToShortIdTopic.readKeyValuesToList();
        assertThat(mapping).hasSize(1);
        assertThat(mapping.get(0).key).isEqualTo("id001");
        assertThat(mapping.get(0).value).isEqualTo(1);
    }

    @Test
    void shouldEmitShortIdToLongIdMapping() {
        longIdTopic.pipeInput("id001", -1);

        final List<KeyValue<Integer, String>> mapping = shortIdToLongIdTopic.readKeyValuesToList();
        assertThat(mapping).hasSize(1);
        assertThat(mapping.get(0).key).isEqualTo(1);
        assertThat(mapping.get(0).value).isEqualTo("id001");
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("shortIdRegistryStreamsBuilder")
        StreamsBuilder shortIdRegistryStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setShortIdRegistryAppId("short-id-registry-topology-test");
            properties.setLongIdTopic(LONG_ID_TOPIC);
            properties.setLongIdToShortIdTopic(LONG_ID_TO_SHORT_ID_TOPIC);
            properties.setShortIdToLongIdTopic(SHORT_ID_TO_LONG_ID_TOPIC);

            return properties;
        }

        @Bean
        KafkaProperties kafkaProperties() {
            KafkaProperties properties = new KafkaProperties();
            properties.getProperties().put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
            return properties;
        }
    }

}
