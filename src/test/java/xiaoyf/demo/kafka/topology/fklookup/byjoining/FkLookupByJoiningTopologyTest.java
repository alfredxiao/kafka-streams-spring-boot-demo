package xiaoyf.demo.kafka.topology.fklookup.byjoining;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
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
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerNumberExtractor;
import xiaoyf.demo.kafka.topology.fklookup.commons.OrderCustomerJoiner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.CUSTOMER_NAME;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        SharedTopologyConfiguration.class,
        FkLookupByJoiningTopologyTest.TestConfig.class,
        FkLookupByJoiningTopology.class,
        OrderCustomerJoiner.class,
        CustomerNumberExtractor.class,
    }
)
@TestPropertySource(
        properties = {"demo-streams.fk-lookup-by-joining-app-enabled=true"}
)
public class FkLookupByJoiningTopologyTest {
    final static String ORDER_TOPIC = "order";
    final static String CUSTOMER_TOPIC = "customer";
    final static String ORDER_ENRICHED_TOPIC = "order-enriched";

    @Autowired
    @Qualifier("fkLookupByJoiningStreamsBuilder")
    private StreamsBuilder fkLookupByJoiningStreamsBuilder;

    private TestInputTopic<OrderKey, OrderValue> orderTopic;
    private TestInputTopic<CustomerKey, CustomerValue> customerTopic;
    private TestOutputTopic<OrderKey, OrderEnriched> orderEnrichedTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(fkLookupByJoiningStreamsBuilder);

        orderTopic = helper.inputTopic(ORDER_TOPIC);
        customerTopic = helper.inputTopic(CUSTOMER_TOPIC);
        orderEnrichedTopic = helper.outputTopic(ORDER_ENRICHED_TOPIC);
    }

    @AfterEach
    void cleanup() {
        if (helper != null) {
            helper.close();
        };
    }

    @Test
    void shouldEnrichOrderWithCustomer() {
        customerTopic.pipeInput(
                testCustomerKey(),
                testCustomerValue()
        );
        orderTopic.pipeInput(
                testOrderKey(),
                testOrderValue()
        );

        final List<KeyValue<OrderKey, OrderEnriched>> ordersEnriched = orderEnrichedTopic.readKeyValuesToList();
        assertThat(ordersEnriched).hasSize(1);
        assertThat(ordersEnriched.get(0).value).isNotNull();
        assertThat(ordersEnriched.get(0).value.getCustomer()).isNotNull();
        assertThat(ordersEnriched.get(0).value.getCustomer().getName()).isEqualTo(CUSTOMER_NAME);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("fkLookupByJoiningStreamsBuilder")
        StreamsBuilder fkLookupByJoiningStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setFkLookupByJoiningAppId("fklookup-by-joining-topology-test");
            properties.setOrderTopic(ORDER_TOPIC);
            properties.setCustomerTopic(CUSTOMER_TOPIC);
            properties.setOrderEnrichedByJoiningTopic(ORDER_ENRICHED_TOPIC);

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
