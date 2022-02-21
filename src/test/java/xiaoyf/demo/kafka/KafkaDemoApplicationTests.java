package xiaoyf.demo.kafka;

import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.FileSystemUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;
import static xiaoyf.demo.kafka.helper.Const.ANOTHER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.ANOTHER_TOPIC_DOUBLED;
import static xiaoyf.demo.kafka.helper.Const.CLICK_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Dumper.dumpAllBeans;
import static xiaoyf.demo.kafka.helper.Dumper.dumpTopicAndSchemaList;
import static xiaoyf.demo.kafka.helper.Dumper.dumpTopology;
import static xiaoyf.demo.kafka.helper.Fixtures.customerDetail;
import static xiaoyf.demo.kafka.helper.Fixtures.customerOrder;
import static xiaoyf.demo.kafka.helper.Fixtures.premiumOrder;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = {
                CUSTOMER_ORDER_TOPIC, CUSTOMER_DETAIL_TOPIC, PREMIUM_ORDER_TOPIC, CLICK_TOPIC, LOCATION_TOPIC,
                ANOTHER_TOPIC, ANOTHER_TOPIC_DOUBLED
        },
        brokerProperties = {
                "log.dirs=./build/kafka-logs",
                "log.cleaner.enabled=false",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        }
)
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@Slf4j
class KafkaDemoApplicationTests {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    // below bean is needed only if we want to produce different types of records
    //@Autowired
    //private KafkaTemplate<Long, Long> longKafkaTemplate;

    @Autowired
    private TestBeans testBeans;

    @Autowired
    private ApplicationContext appContext;

    @AfterEach
    void cleanup() throws IOException {
        FileSystemUtils.deleteRecursively(Path.of("./build/tmp/state-dir"));
    }

    @Test
    void shouldRunAllStreamProcessing() throws Exception {
        dumpAllBeans(appContext);

        // firstly, the table-table fk join
        var order = customerOrder(123, 100, "iPhone", new BigDecimal("1500"), "GoHigh");
        kafkaTemplate.send(CUSTOMER_ORDER_TOPIC, order.key(), order.value()).get();

        var details = customerDetail(100, "Alfred", "a@g.com", "GoHigh", "GoFar");
        kafkaTemplate.send(CUSTOMER_DETAIL_TOPIC, details.key(), details.value()).get();

        // secondly, the

        // thirdly, another topology
        kafkaTemplate.send(ANOTHER_TOPIC, 1L, 3L);

        // verify premium order
		    Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() -> testBeans.premiumOrders.size() == 1);
        var actualPremium = testBeans.premiumOrders.get(0);
        var expectedPremium = premiumOrder(123, 100, "iPhone", new BigDecimal("1500"), "GoHigh", "Alfred", "a@g.com");
        assertThat(actualPremium.key()).isEqualTo(expectedPremium.key);
        assertThat(actualPremium.value()).isEqualTo(expectedPremium.value);

        // verify doubled number from another topology
        Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() -> testBeans.doubled.size() == 1);
        var actualNum = testBeans.doubled.get(0);
        assertThat(actualNum.key()).isEqualTo(1L);
        assertThat(actualNum.value()).isEqualTo(6L);

        dumpTopicAndSchemaList();
    }

    @TestConfiguration
    @Slf4j
    public static class TestBeans {
        @Autowired
        private EmbeddedKafkaBroker broker;

        @Autowired
        @Qualifier("defaultKafkaStreamsBuilder")
        private StreamsBuilder streamsBuilder;

        @Autowired
        @Qualifier("anotherStreamBuilder")
        private StreamsBuilder anotherStreamBuilder;

        private final List<ConsumerRecord<PremiumOrderKey, PremiumOrder>> premiumOrders = new ArrayList<>();
        private final List<ConsumerRecord<Long, Long>> doubled = new ArrayList<>();

        @KafkaListener(topics = PREMIUM_ORDER_TOPIC)
        public void consumePremiumOrder(final ConsumerRecord<PremiumOrderKey, PremiumOrder> record, Acknowledgment ack) {
            log.info("Seen PremiumOrder record: {}", record);
            this.premiumOrders.add(record);
            ack.acknowledge();
        }

        @KafkaListener(
                topics = ANOTHER_TOPIC_DOUBLED,
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

        // Below four beans are needed if we want to produce different types of records
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
            dumpTopology("defaultKafkaStreamsBuilder", streamsBuilder.build());
            dumpTopology("anotherStreamBuilder", anotherStreamBuilder.build());
        }
    }
}
