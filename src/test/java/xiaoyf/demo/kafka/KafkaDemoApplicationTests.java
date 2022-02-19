package xiaoyf.demo.kafka;

import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
import xiaoyf.demo.kafka.helper.serde.SharedMockSchemaRegistryClient;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;
import static xiaoyf.demo.kafka.helper.Const.*;
import static xiaoyf.demo.kafka.helper.Dumper.dumpAllBeans;
import static xiaoyf.demo.kafka.helper.Dumper.dumpTopicAndSchemaList;
import static xiaoyf.demo.kafka.helper.Dumper.dumpTopology;
import static xiaoyf.demo.kafka.helper.Fixtures.*;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = {CUSTOMER_ORDER_TOPIC, CUSTOMER_DETAIL_TOPIC, PREMIUM_ORDER_TOPIC, CLICK_TOPIC, LOCATION_TOPIC},
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

    @Autowired
    private TestListeners testListeners;

    @Autowired
    private ApplicationContext appContext;

    @AfterEach
    void cleanup() throws IOException {
        FileSystemUtils.deleteRecursively(Path.of("./build/tmp/state-dir"));
    }

    @Test
    void shouldJoinOrdersWithCustomerDetails() throws Exception {
        dumpAllBeans(appContext);

        var order = customerOrder(123, 100, "iPhone", "1500", "GoHigh");
        kafkaTemplate.send(CUSTOMER_ORDER_TOPIC, order.key(), order.value()).get();

        var details = customerDetail(100, "Alfred", "a@g.com", "GoHigh", "GoFar");
        kafkaTemplate.send(CUSTOMER_DETAIL_TOPIC, details.key(), details.value()).get();

		    Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() ->
				    testListeners.outputs.size() == 1);

        dumpTopicAndSchemaList();

		    var output = testListeners.outputs.get(0);
        var premium = premiumOrder(123, 100, "iPhone", "1500", "GoHigh", "Alfred", "a@g.com");

        assertThat(output.key()).isEqualTo(premium.key);
        assertThat(output.value()).isEqualTo(premium.value);
    }

    @Test
    void shouldNotJoinOrdersWithCustomerDetailsWhenCampaignDoesNotMatch() throws Exception {
        var order = customerOrder(567, 100, "Galaxy", "1200", "GoHigh");
        kafkaTemplate.send(CUSTOMER_ORDER_TOPIC, order.key(), order.value()).get();

        var details = customerDetail(100, "Alfred", "a@g.com", "GoFar");
        kafkaTemplate.send(CUSTOMER_DETAIL_TOPIC, details.key(), details.value()).get();

		    Awaitility.await().timeout(20, TimeUnit.SECONDS).until(() ->
				    testListeners.outputs.size() == 0);
    }

    @TestConfiguration
    @Slf4j
    public static class TestListeners {
        @Autowired
        private EmbeddedKafkaBroker broker;

        @Autowired
        private StreamsBuilder streamsBuilder;

        private final List<ConsumerRecord<PremiumOrderKey, PremiumOrder>> outputs = new ArrayList<>();

        @KafkaListener(topics = PREMIUM_ORDER_TOPIC)
        public void consumeCustomerEvent(final ConsumerRecord<PremiumOrderKey, PremiumOrder> record, Acknowledgment ack) {
            log.info("Seen output record: {}", record);
            this.outputs.add(record);
            ack.acknowledge();
        }

        @PostConstruct
        public void init() {
            // just to demonstrate how to configure the default broker
            broker.setAdminTimeout(30);

            // dump the topology, build()
            dumpTopology(streamsBuilder.build());
        }
    }
}
