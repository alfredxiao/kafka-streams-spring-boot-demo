package xiaoyf.demo.kafka.processor;

import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import xiaoyf.demo.kafka.filter.BigCampaignPurchase;
import xiaoyf.demo.kafka.helper.serde.MockSpecificAvroSerde;
import xiaoyf.demo.kafka.joiner.PremiumTransactionValueJoiner;
import xiaoyf.demo.kafka.mapper.PremiumOrderKeyMapper;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.APPLICATION_ID;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAILS_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Dumper.*;
import static xiaoyf.demo.kafka.helper.Fixtures.customerDetail;
import static xiaoyf.demo.kafka.helper.Fixtures.customerOrder;
import static xiaoyf.demo.kafka.helper.Fixtures.premiumOrder;

@SpringBootTest(classes = {
        TransactionProcessor.class,
        BigCampaignPurchase.class,
        PremiumTransactionValueJoiner.class,
        PremiumOrderKeyMapper.class
})
@Import(TransactionProcessorTest.ProcessorConfig.class)
@Tag("IntegratedUnitTest")
class TransactionProcessorTest {

    @Autowired
    StreamsBuilder streamsBuilder;

    private Serde<CustomerOrderKey> customerOrderKeySerde;
    private Serde<CustomerOrder> customerOrderSerde;
    private Serde<CustomerDetailsKey> customerDetailsKeySerde;
    private Serde<CustomerDetails> customerDetailsSerde;
    private Serde<PremiumOrderKey> premiumOrderKeySerde;
    private Serde<PremiumOrder> premiumOrderSerde;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        Serde<?> singleton = new MockSpecificAvroSerde<>();
        customerOrderKeySerde = (Serde<CustomerOrderKey>) singleton;
        customerOrderSerde = (Serde<CustomerOrder>) singleton;
        customerDetailsKeySerde = (Serde<CustomerDetailsKey>) singleton;
        customerDetailsSerde = (Serde<CustomerDetails>) singleton;
        premiumOrderKeySerde = (Serde<PremiumOrderKey>) singleton;
        premiumOrderSerde = (Serde<PremiumOrder>) singleton;
    }


    @Test
    void shouldRaisePremiumOrder() throws Exception {
        final var custOrders = List.of(
                customerOrder(123, 100, "iPhone", "1500", "GoHigh")
        );
        final var custDetails = List.of(
                customerDetail(100, "Alfred", "a@g.com", "GoHigh", "GoFar")
        );

        final var premiumOrders = List.of(
                premiumOrder(123, 100, "iPhone", "1500", "GoHigh", "Alfred", "a@g.com")
        );

        verify(custOrders, custDetails, premiumOrders);

        System.out.println(custDetails);

    }

    void verify(
            List<TestRecord<CustomerOrderKey, CustomerOrder>> custOrders,
            List<TestRecord<CustomerDetailsKey, CustomerDetails>> custDetails,
            List<KeyValue<PremiumOrderKey, PremiumOrder>> premOrders
    ) throws Exception {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MockSpecificAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MockSpecificAvroSerde.class);

        final Topology topology = streamsBuilder.build(props);

        dumpTopology(topology);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {
            final TestInputTopic<CustomerOrderKey, CustomerOrder> orders = testDriver
                    .createInputTopic(CUSTOMER_ORDER_TOPIC,
                            customerOrderKeySerde.serializer(),
                            customerOrderSerde.serializer());
            final TestInputTopic<CustomerDetailsKey, CustomerDetails> details = testDriver
                    .createInputTopic(CUSTOMER_DETAILS_TOPIC,
                            customerDetailsKeySerde.serializer(),
                            customerDetailsSerde.serializer());
            final TestOutputTopic<PremiumOrderKey, PremiumOrder> output = testDriver
                    .createOutputTopic(PREMIUM_ORDER_TOPIC,
                            premiumOrderKeySerde.deserializer(),
                            premiumOrderSerde.deserializer());

            orders.pipeRecordList(custOrders);
            details.pipeRecordList(custDetails);

            dumpTestDriverStats(testDriver);
            dumpTopicAndSchemaList();

            assertThat(output.readKeyValuesToList()).isEqualTo(premOrders);
        }
    }

    @TestConfiguration
    static class ProcessorConfig {
        @Bean
        StreamsBuilder streamsBuilder() {
            return new StreamsBuilder();
        }
    }
}
