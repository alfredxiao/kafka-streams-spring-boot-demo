package xiaoyf.demo.kafka;

import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import xiaoyf.demo.kafka.helper.serde.SharedMockSchemaRegistryClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.*;
import static xiaoyf.demo.kafka.helper.Fixtures.*;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        topics = {CUSTOMER_ORDER_TOPIC, CUSTOMER_DETAILS_TOPIC, PREMIUM_ORDER_TOPIC},
        brokerProperties = {
                "log.dirs=./build/kafka-logs",
                "log.cleaner.enabled=false",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        }
)
@Slf4j
class KafkaDemoApplicationTests {

    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    TestListeners testListeners;

    @Test
    void shouldJoinOrdersWithCustomerDetails() throws Exception {
        var order = customerOrder(123, 100, "iPhone", "1500", "GoHigh");
        kafkaTemplate.send(CUSTOMER_ORDER_TOPIC, order.key(), order.value()).get();

        var details = customerDetail(100, "Alfred", "a@g.com", "GoHigh", "GoFar");
        kafkaTemplate.send(CUSTOMER_DETAILS_TOPIC, details.key(), details.value()).get();

		    Awaitility.await().timeout(36, TimeUnit.SECONDS).until(() ->
				    testListeners.outputs.size() == 1);

		    var output = testListeners.outputs.get(0);
        var premium = premiumOrder(123, 100, "iPhone", "1500", "GoHigh", "Alfred", "a@g.com");

        assertThat(output.key()).isEqualTo(premium.key);
        assertThat(output.value()).isEqualTo(premium.value);

        log.info("!!! SUBJECT LIST");
        SharedMockSchemaRegistryClient.getInstance().getAllSubjects().stream().sorted().forEach(log::info);
        SharedMockSchemaRegistryClient.getInstance().getAllSubjects()
                .forEach(subject -> {
                    try {
                        log.info("!!! SUBJECT= {}", subject);
                        var meta = SharedMockSchemaRegistryClient.getInstance().getLatestSchemaMetadata(subject);
                        log.info("!!! id = {}, schema = {}", meta.getId(), meta.getSchema());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    @TestConfiguration
    @Slf4j
    public static class TestListeners {
        private final List<ConsumerRecord<PremiumOrderKey, PremiumOrder>> outputs = new ArrayList<>();

        @KafkaListener(topics = PREMIUM_ORDER_TOPIC)
        public void consumeCustomerEvent(final ConsumerRecord<PremiumOrderKey, PremiumOrder> record, Acknowledgment ack) {
            log.info("Seen output record: {}", record);
            this.outputs.add(record);
            ack.acknowledge();
        }

    }
}
