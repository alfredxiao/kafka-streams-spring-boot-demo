package xiaoyf.demo.kafka.integration;

import demo.model.CustomerKey;
import demo.model.EnrichedPreferenceValue;
import demo.model.OrderEnriched;
import demo.model.OrderKey;
import demo.model.OrderValue;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import xiaoyf.demo.kafka.helper.consumer.TestConsumer;
import xiaoyf.demo.kafka.helper.serde.AnyDeserializer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static xiaoyf.demo.kafka.helper.data.TestData.testContactValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceValue;

@SpringBootTest(
        properties = {"spring.kafka.streams.application-id=intg-test-${random.uuid}"}
)
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = {
                SpringBootKafkaStreamsDemoApplicationTest.ORDER_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.CUSTOMER_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.ORDER_DEDUPED_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.ORDER_ENRICHED_BY_GLOBAL_STORE_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.ORDER_ENRICHED_BY_GLOBAL_KTABLE_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.ORDER_ENRICHED_BY_JOINING_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.PREFERENCE_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.CONTACT_TOPIC,
                SpringBootKafkaStreamsDemoApplicationTest.PREFERENCE_ENRICHED_TOPIC,
        },
        brokerProperties = {
                "log.dirs=./build/kafka-logs",
                "log.cleaner.enabled=false",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "transaction.state.log.num.partitions=1",
                "auto.create.topics.enable=true",
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@Slf4j
class SpringBootKafkaStreamsDemoApplicationTest {
    public static final String ORDER_TOPIC = "order";
    public static final String ORDER_DEDUPED_TOPIC = "order-deduped";
    public static final String CUSTOMER_TOPIC = "customer";
    public static final String ORDER_ENRICHED_BY_GLOBAL_STORE_TOPIC = "order-enriched-by-global-store";
    public static final String ORDER_ENRICHED_BY_GLOBAL_KTABLE_TOPIC = "order-enriched-by-global-ktable";
    public static final String ORDER_ENRICHED_BY_REGULAR_STORE_TOPIC = "order-enriched-by-regular-store";
    public static final String ORDER_ENRICHED_BY_JOINING_TOPIC = "order-enriched-by-joining";
    public static final String PREFERENCE_TOPIC = "preference";
    public static final String CONTACT_TOPIC = "contact";
    public static final String PREFERENCE_ENRICHED_TOPIC = "preference-enriched";
    private static final Duration MAX_WAIT_IN_TEST_CONSUMER = Duration.ofSeconds(15);
    private static final Duration MIN_WAIT_IN_TEST_CONSUMER = Duration.ofSeconds(3);
    private static final String AVRO_DESERIALIZER_CLASS_NAME = AnyDeserializer.class.getName();

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    public int getBrokerPort() {
        String brokers = embeddedKafkaBroker.getBrokersAsString();
        String[] brokerAddresses = brokers.split(",");
        String firstBrokerAddress = brokerAddresses[0];
        return Integer.parseInt(firstBrokerAddress.split(":")[1]);
    }

    @BeforeAll
    static void setup() {
        Path.of("./build/tmp/state-dir").toFile().mkdirs();
    }

    @AfterAll
    static void cleanup() throws Exception {
        //FileSystemUtils.deleteRecursively(Path.of("./build/tmp/state-dir"));
    }

    @Test
    void shouldDedupe() throws Exception {
        final Long orderNumber = 101L;
        kafkaTemplate.send(
                ORDER_TOPIC,
                testOrderKey(orderNumber),
                testOrderValue(orderNumber)
        ).get();

        TestConsumer<OrderKey, OrderValue> testConsumer = new TestConsumer<OrderKey, OrderValue>(
                getBrokerPort(),
                ORDER_DEDUPED_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<OrderKey, OrderValue>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).key().getOrderNumber()).isEqualTo(orderNumber);
                });
    }

    @Test
    void shouldLookupCustomerViaGlobalStore() throws Exception {
        final Long orderNumber = 102L;
        kafkaTemplate.send(
                ORDER_TOPIC,
                testOrderKey(orderNumber),
                testOrderValue(orderNumber)
        ).get();

        TestConsumer<OrderKey, OrderEnriched> testConsumer = new TestConsumer<OrderKey, OrderEnriched>(
                getBrokerPort(),
                ORDER_ENRICHED_BY_GLOBAL_STORE_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<OrderKey, OrderEnriched>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).key().getOrderNumber()).isEqualTo(orderNumber);
                });
    }

    @Test
    void shouldLookupCustomerViaGlobalKTable() throws Exception {
        final Long orderNumber = 103L;
        kafkaTemplate.send(
                ORDER_TOPIC,
                testOrderKey(orderNumber),
                testOrderValue(orderNumber)
        ).get();

        TestConsumer<OrderKey, OrderEnriched> testConsumer = new TestConsumer<OrderKey, OrderEnriched>(
                getBrokerPort(),
                ORDER_ENRICHED_BY_GLOBAL_KTABLE_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<OrderKey, OrderEnriched>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).key().getOrderNumber()).isEqualTo(orderNumber);
                });
    }

    @Test
    void shouldLookupCustomerViaRegularStore() throws Exception {
        final Long orderNumber = 104L;
        kafkaTemplate.send(
                ORDER_TOPIC,
                testOrderKey(orderNumber),
                testOrderValue(orderNumber)
        ).get();

        TestConsumer<OrderKey, OrderEnriched> testConsumer = new TestConsumer<OrderKey, OrderEnriched>(
                getBrokerPort(),
                ORDER_ENRICHED_BY_REGULAR_STORE_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<OrderKey, OrderEnriched>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).key().getOrderNumber()).isEqualTo(orderNumber);
                });
    }

    @Test
    void shouldLookupCustomerViaJoining() throws Exception {
        final Long orderNumber = 105L;
        kafkaTemplate.send(
                ORDER_TOPIC,
                testOrderKey(orderNumber),
                testOrderValue(orderNumber)
        ).get();

        TestConsumer<OrderKey, OrderEnriched> testConsumer = new TestConsumer<OrderKey, OrderEnriched>(
                getBrokerPort(),
                ORDER_ENRICHED_BY_JOINING_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<OrderKey, OrderEnriched>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).key().getOrderNumber()).isEqualTo(orderNumber);
                });
    }

    @Test
    void shouldDualJoin() throws Exception {
        kafkaTemplate.send(
                PREFERENCE_TOPIC,
                testPreferenceKey(),
                testPreferenceValue()
        ).get();

        kafkaTemplate.send(
                CONTACT_TOPIC,
                testCustomerKey(),
                testContactValue()
        ).get();

        TestConsumer<CustomerKey, EnrichedPreferenceValue> testConsumer = new TestConsumer<CustomerKey, EnrichedPreferenceValue>(
                getBrokerPort(),
                PREFERENCE_ENRICHED_TOPIC,
                AVRO_DESERIALIZER_CLASS_NAME,
                AVRO_DESERIALIZER_CLASS_NAME)
                .startListening(MIN_WAIT_IN_TEST_CONSUMER, MAX_WAIT_IN_TEST_CONSUMER, 1);

        await().atMost(MAX_WAIT_IN_TEST_CONSUMER)
                .untilAsserted(() -> {
                    List<ConsumerRecord<CustomerKey, EnrichedPreferenceValue>> received = testConsumer.getReceivedRecords();
                    assertThat(received).hasSize(1);
                    assertThat(received.get(0).value().getContact()).isNotNull();
                });
    }

    @TestConfiguration
    @Slf4j
    public static class TestBeans {
        @Autowired
        private KafkaTemplate<Object, Object> kafkaTemplate;

        @PostConstruct
        public void init() throws Exception {
            kafkaTemplate.send(
                    CUSTOMER_TOPIC,
                    testCustomerKey(),
                    testCustomerValue()
            ).get();
        }
    }
}
