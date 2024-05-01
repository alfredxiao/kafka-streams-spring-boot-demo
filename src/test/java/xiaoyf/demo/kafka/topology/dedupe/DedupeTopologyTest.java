package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderValue;
import demo.model.OrderKey;
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
import org.springframework.test.context.junit.jupiter.SpringExtension;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.config.SharedTopologyConfiguration;
import xiaoyf.demo.kafka.helper.testhelper.TopologyTestHelper;


import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        SharedTopologyConfiguration.class,
        DedupeTopologyTest.TestConfig.class,
        DedupeTopology.class,
        DedupeTopologyConfiguration.class,
    }
)
public class DedupeTopologyTest {
    final static String ORDER_TOPIC = "order";
    final static String ORDER_DEDUPED_TOPIC = "order-deduped";

    @Autowired
    @Qualifier("dedupeStreamsBuilder")
    private StreamsBuilder dedupeStreamsBuilder;

    private TestInputTopic<OrderKey, OrderValue> orderTopic;
    private TestOutputTopic<OrderKey, OrderValue> dedupedOrderTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(dedupeStreamsBuilder);

        orderTopic = helper.inputTopic(ORDER_TOPIC);
        dedupedOrderTopic = helper.outputTopic(ORDER_DEDUPED_TOPIC);
    }

    @AfterEach
    void cleanup() {
        if (helper != null) {
            helper.close();
        };
    }

    @Test
    void shouldDedupe() {
        orderTopic.pipeInput(
                testOrderKey(),
                testOrderValue()
        );
        orderTopic.pipeInput(
                testOrderKey(),
                testOrderValue()
        );

        final List<KeyValue<OrderKey, OrderValue>> jobTypeDescOutputs = dedupedOrderTopic.readKeyValuesToList();
        assertThat(jobTypeDescOutputs).hasSize(1);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("dedupeStreamsBuilder")
        StreamsBuilder dedupeStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setDedupeAppId("dedupe-topology-test");
            properties.setOrderTopic(ORDER_TOPIC);
            properties.setOrderDedupedTopic(ORDER_DEDUPED_TOPIC);

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
