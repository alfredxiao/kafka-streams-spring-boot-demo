package xiaoyf.demo.kafka.integration;

import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.FileSystemUtils;
import xiaoyf.demo.kafka.helper.serde.MockSerde;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.CLICK_PLUS_LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CLICK_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_DOUBLED_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_CATEGORISED_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_CATEGORY_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_TRANSACTION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.SELF_JOIN_INPUT_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.SELF_JOIN_OUTPUT_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM1_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM2_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM_MERGED_TOPIC;
import static xiaoyf.demo.kafka.helper.Dumper.dumpAllBeans;
import static xiaoyf.demo.kafka.helper.Dumper.dumpTopicAndSchemaList;
import static xiaoyf.demo.kafka.helper.Fixtures.customerDetail;
import static xiaoyf.demo.kafka.helper.Fixtures.customerOrder;
import static xiaoyf.demo.kafka.helper.Fixtures.premiumOrder;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = {
                CUSTOMER_ORDER_TOPIC, CUSTOMER_DETAIL_TOPIC, PREMIUM_ORDER_TOPIC,
                CLICK_TOPIC, LOCATION_TOPIC, CLICK_PLUS_LOCATION_TOPIC,
                MCC_TRANSACTION_TOPIC, MCC_CATEGORY_TOPIC, MCC_CATEGORISED_TOPIC,
                LONG_NUMBER_TOPIC, LONG_NUMBER_DOUBLED_TOPIC,
                STREAM1_TOPIC, STREAM2_TOPIC, STREAM_MERGED_TOPIC,
                SELF_JOIN_INPUT_TOPIC, SELF_JOIN_OUTPUT_TOPIC
        },
        brokerProperties = {
                "log.dirs=./build/kafka-logs",
                "log.cleaner.enabled=false",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        }
)
@Slf4j
class SpringBootKafkaStreamsDemoApplicationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    // below bean is needed only if we want to produce different types of records, also need to uncomment below lines
    // regarding ProducerFactory and beans for both <Object, Object> and <Long, Long> types.
    //@Autowired
    //private KafkaTemplate<Long, Long> longKafkaTemplate;

    @Autowired
    private TestBeans testBeans;

    @Autowired
    private ApplicationContext appContext;

    @BeforeAll
    static void setup() throws IOException {
        Path.of("./build/tmp/state-dir").toFile().mkdirs();
    }

    @AfterAll
    static void cleanup() throws IOException {
        FileSystemUtils.deleteRecursively(Path.of("./build/tmp/state-dir"));
    }

    @Test
    void shouldRunMerge() throws Exception {
        kafkaTemplate.send(STREAM1_TOPIC, "k1", "v1").get();
        kafkaTemplate.send(STREAM2_TOPIC, "k2", "v2").get();

        Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() -> testBeans.mergedStream.size() == 2);
    }

    @Test
    void shouldRunTableTableForeignKeyJoin() throws Exception {
        var order = customerOrder(123, 100, "iPhone", new BigDecimal("1500"), "GoHigh");
        kafkaTemplate.send(CUSTOMER_ORDER_TOPIC, order.key(), order.value()).get();
        var details = customerDetail(100, "Alfred", "a@g.com", "GoHigh", "GoFar");
        kafkaTemplate.send(CUSTOMER_DETAIL_TOPIC, details.key(), details.value()).get();

        Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() -> testBeans.premiumOrders.size() == 1);
        var actualPremium = testBeans.premiumOrders.get(0);
        var expectedPremium = premiumOrder(123, 100, "iPhone", new BigDecimal("1500"), "GoHigh", "Alfred", "a@g.com");
        assertThat(actualPremium.key()).isEqualTo(expectedPremium.key);
        assertThat(actualPremium.value()).isEqualTo(expectedPremium.value);
    }

    @Test
    void shouldRunStreamTableJoin() throws Exception {
        kafkaTemplate.send(LOCATION_TOPIC, "101", "Melbourne").get();
        kafkaTemplate.send(CLICK_TOPIC, "101", "Amazon").get();

        Awaitility.await().timeout(36, TimeUnit.SECONDS).until(() -> testBeans.clickPlusLocation.size() == 1);
        assertThat(testBeans.clickPlusLocation.get(0).key()).isEqualTo("101");
        assertThat(testBeans.clickPlusLocation.get(0).value()).isEqualTo("Amazon accessed from Melbourne");
    }

    @Test
    void shouldRunAnotherTopology() throws Exception {
        dumpAllBeans(appContext);

        kafkaTemplate.send(LONG_NUMBER_TOPIC, 1L, 3L);

        Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() -> testBeans.doubled.size() == 1);
        var actualNum = testBeans.doubled.get(0);
        assertThat(actualNum.key()).isEqualTo(1L);
        assertThat(actualNum.value()).isEqualTo(6L);

        // order of the two record to be sent is very important
        // category record has to go first to make a join happen
        kafkaTemplate.send(MCC_CATEGORY_TOPIC, "key001", "Airline").get();
        kafkaTemplate.send(MCC_TRANSACTION_TOPIC, "key001", "3001").get();

        Awaitility.await().timeout(36, TimeUnit.SECONDS).until(() -> testBeans.mccCategorised.size() == 1);
        assertThat(testBeans.mccCategorised.get(0).key()).isEqualTo("key001");
        assertThat(testBeans.mccCategorised.get(0).value()).isEqualTo("3001 is Airline");

        dumpTopicAndSchemaList();
    }

    @TestConfiguration
    @Slf4j
    public static class TestBeans {
        @Bean
        @Primary
        Serde<String> testStringSerde() {
            return new MockSerde<>();
        }

        @Autowired
        private EmbeddedKafkaBroker broker;

        @Autowired
        @Qualifier("defaultKafkaStreamsBuilder")
        private StreamsBuilder streamsBuilder;

        @Autowired
        @Qualifier("secondaryKafkaStreamBuilder")
        private StreamsBuilder secondaryKafkaStreamBuilder;

        private final List<ConsumerRecord<PremiumOrderKey, PremiumOrder>> premiumOrders = new ArrayList<>();
        private final List<ConsumerRecord<String, String>> clickPlusLocation = new ArrayList<>();
        private final List<ConsumerRecord<String, String>> mccCategorised = new ArrayList<>();
        private final List<ConsumerRecord<Long, Long>> doubled = new ArrayList<>();
        private final List<ConsumerRecord<String, String>> mergedStream = new ArrayList<>();

        @KafkaListener(topics = PREMIUM_ORDER_TOPIC)
        public void consumePremiumOrder(final ConsumerRecord<PremiumOrderKey, PremiumOrder> record, Acknowledgment ack) {
            log.info("Seen PremiumOrder record: {}", record);
            this.premiumOrders.add(record);
            ack.acknowledge();
        }

        @KafkaListener(topics = CLICK_PLUS_LOCATION_TOPIC)
        public void consumeClickPlusLocation(final ConsumerRecord<String, String> record, Acknowledgment ack) {
            log.info("Seen click plus location record: {}", record);
            this.clickPlusLocation.add(record);
            ack.acknowledge();
        }

        @KafkaListener(topics = MCC_CATEGORISED_TOPIC)
        public void consumeClickPlusLocationGlobalKTable(final ConsumerRecord<String, String> record, Acknowledgment ack) {
            log.info("Seen mcc-categorised record: {}", record);
            this.mccCategorised.add(record);
            ack.acknowledge();
        }

        @KafkaListener(
                topics = CLICK_TOPIC,
                properties = {
                        "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
                }
        )
        public void consumeClick(final ConsumerRecord<String, String> record, Acknowledgment ack) {
            log.info("Seen click record: {}", record);
            //this.clickPlusLocation.add(record);
            ack.acknowledge();
        }

        @KafkaListener(
                topics = LONG_NUMBER_DOUBLED_TOPIC,
                properties = {
                        "key.deserializer=org.apache.kafka.common.serialization.LongDeserializer",
                        "value.deserializer=org.apache.kafka.common.serialization.LongDeserializer"
                }
        )
        public void consumeAnotherTopicDoubled(final ConsumerRecord<Long, Long> record, Acknowledgment ack) {
            log.info("Seen doubled record: {}", record);
            this.doubled.add(record);
            ack.acknowledge();
        }

        @KafkaListener(
                topics = STREAM_MERGED_TOPIC,
                properties = {
                        "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
                }
        )
        public void consumeMergedStream(final ConsumerRecord<String, String> record, Acknowledgment ack) {
            log.info("Seen merged stream record: {}", record);
            this.mergedStream.add(record);
            ack.acknowledge();
        }

        // Below four beans are needed if we want to produce different types of records using two KafkaTemplate
        // instances for different types of key/value
        //@Bean
        //public KafkaTemplate<Object, Object> defaultKafkaTemplate(ProducerFactory<Object, Object> defaultProducerFactory) {
        //    return new KafkaTemplate<>(defaultProducerFactory);
        //}
        //
        //@Bean
        //public ProducerFactory<Object, Object> objectProducerFactory(KafkaProperties kafkaProps) {
        //    return new DefaultKafkaProducerFactory<>(kafkaProps.buildProducerProperties());
        //}

        //@Bean
        //public KafkaTemplate<Long, Long> longKafkaTemplate(ProducerFactory<Long, Long> longProducerFactory) {
        //    return new KafkaTemplate<>(longProducerFactory);
        //}
        //
        //@Bean
        //public ProducerFactory<Long, Long> longProducerFactory(KafkaProperties kafkaProps) {
        //    Map<String, Object> producerProperties = kafkaProps.buildProducerProperties();
        //    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        //    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        //
        //    return new DefaultKafkaProducerFactory<>(producerProperties);
        //}

        @PostConstruct
        public void init() {
            // just to demonstrate how to configure the default broker
            broker.setAdminTimeout(30);

            // dump the topology, build()
//            dumpTopology("defaultKafkaStreamsBuilder", streamsBuilder.build());
//            dumpTopology("secondaryKafkaStreamBuilder", secondaryKafkaStreamBuilder.build());
        }
    }
}
