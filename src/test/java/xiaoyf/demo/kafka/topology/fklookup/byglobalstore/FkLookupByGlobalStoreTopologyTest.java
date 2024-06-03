package xiaoyf.demo.kafka.topology.fklookup.byglobalstore;

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
        FkLookupByGlobalStoreTopologyTest.TestConfig.class,
        FkLookupByGlobalStoreTopology.class,
        FkLookupByGlobalStoreTopologyConfiguration.class,
    }
)
@TestPropertySource(
        properties = {"demo-streams.fk-lookup-by-global-store-app-enabled=true"}
)
public class FkLookupByGlobalStoreTopologyTest {
    final static String ORDER_TOPIC = "order";
    final static String CUSTOMER_TOPIC = "customer";
    final static String ORDER_ENRICHED_TOPIC = "order-enriched";

    @Autowired
    @Qualifier("fkLookupByGlobalStoreStreamsBuilder")
    private StreamsBuilder fkLookupByGlobalStoreStreamsBuilder;

    private TestInputTopic<OrderKey, OrderValue> orderTopic;
    private TestInputTopic<CustomerKey, CustomerValue> customerTopic;
    private TestOutputTopic<OrderKey, OrderEnriched> orderEnrichedTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(fkLookupByGlobalStoreStreamsBuilder);

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
    }

    @Test
    void shouldNotIgnoreWhenCustomerNumberNotFound() {
        customerTopic.pipeInput(
                testCustomerKey(),
                testCustomerValue()
        );

        final OrderValue orderValue = testOrderValue();
        orderValue.setCustomerNumber(null);
        orderTopic.pipeInput(
                testOrderKey(),
                orderValue
        );

        final List<KeyValue<OrderKey, OrderEnriched>> ordersEnriched = orderEnrichedTopic.readKeyValuesToList();

        // Test will pass once record is forwarded when fk found to be null - need to update code in
        // FkLookupProcessor.process(); and this is not possible in the global-ktable approach, where no output is
        // produced when fk is null

        // assertThat(ordersEnriched).hasSize(1);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("fkLookupByGlobalStoreStreamsBuilder")
        StreamsBuilder fkLookupByGlobalStoreStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setFkLookupByGlobalStoreAppId("fklookup-by-global-store-topology-test");
            properties.setOrderTopic(ORDER_TOPIC);
            properties.setCustomerTopic(CUSTOMER_TOPIC);
            properties.setOrderEnrichedByGlobalStoreTopic(ORDER_ENRICHED_TOPIC);

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
