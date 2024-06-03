package xiaoyf.demo.kafka.topology.scheduledjob;

import demo.model.CustomerOrderBatchKey;
import demo.model.CustomerOrderBatchValue;
import demo.model.OrderEnriched;
import demo.model.OrderKey;
import demo.model.OrderValue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.CUSTOMER_NUMBER;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        classes = {
                SharedTopologyConfiguration.class,
                WallClockBatchingTopologyTest.TestConfig.class,
                WallClockBatchingTopology.class,
        }
)
@TestPropertySource(
        properties = {"demo-streams.scheduled-job-app-enabled=true"}
)
public class WallClockBatchingTopologyTest {

    final static Duration WINDOW_SIZE = Duration.ofMinutes(5);
    final static String ORDER_TOPIC = "order";
    final static String CUSTOMER_ORDER_BATCH = "customer-order-batch";

    @Autowired
    @Qualifier("scheduledStreamsBuilder")
    private StreamsBuilder scheduledStreamsBuilder;

    private TopologyTestHelper helper;

    TestInputTopic<OrderKey, OrderValue> orderTopic;
    TestOutputTopic<CustomerOrderBatchKey, CustomerOrderBatchValue> customerOrderBatchTopic;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(scheduledStreamsBuilder, Instant.ofEpochMilli(1000));

        orderTopic = helper.inputTopic(ORDER_TOPIC);
        customerOrderBatchTopic = helper.outputTopic(CUSTOMER_ORDER_BATCH);
    }

    @Test
    public void shouldEmitWindows() {
        final Long orderNumber1 = 101L;
        final Long customerNumber1 = 6001L;

        final Long orderNumber2 = 102L;
        final Long customerNumber2 = 6002L;

        final Long orderNumber3 = 103L;

        orderTopic.pipeInput(
                testOrderKey(orderNumber1),
                testOrderValue(orderNumber1, customerNumber1)
        );
        orderTopic.pipeInput(
                testOrderKey(orderNumber2),
                testOrderValue(orderNumber2, customerNumber2)
        );
        orderTopic.pipeInput(
                testOrderKey(orderNumber3),
                testOrderValue(orderNumber3, customerNumber1)
        );

        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);

        final List<KeyValue<CustomerOrderBatchKey, CustomerOrderBatchValue>> customerOrderBatches =
                customerOrderBatchTopic.readKeyValuesToList();

        assertThat(customerOrderBatches).hasSize(2);

        assertThat(customerOrderBatches).allSatisfy(
                batch -> {
                    assertThat(batch.key.getWinStart().toEpochMilli()).isEqualTo(0);
                    assertThat(batch.key.getWinEnd().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis());
                }
        );

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber1);
                    assertThat(batch.value.getOrders()).hasSize(2);
                }
        );

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber2);
                    assertThat(batch.value.getOrders()).hasSize(1);
                }
        );
    }

    @Test
    public void shouldEmitWindowsContinuously() {
        final Long orderNumber1 = 101L;
        final Long customerNumber1 = 6001L;

        final Long orderNumber2 = 102L;

        orderTopic.pipeInput(
                testOrderKey(orderNumber1),
                testOrderValue(orderNumber1, customerNumber1)
        );

        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);

        orderTopic.pipeInput(
                testOrderKey(orderNumber2),
                testOrderValue(orderNumber2, customerNumber1)
        );

        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);



        final List<KeyValue<CustomerOrderBatchKey, CustomerOrderBatchValue>> customerOrderBatches =
                customerOrderBatchTopic.readKeyValuesToList();

        assertThat(customerOrderBatches).hasSize(2);

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    // first window
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber1);
                    assertThat(batch.key.getWinStart().toEpochMilli()).isEqualTo(0);
                    assertThat(batch.key.getWinEnd().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis());
                    assertThat(batch.value.getOrders()).hasSize(1);
                    assertThat(batch.value.getOrders().get(0).getOrderNumber()).isEqualTo(orderNumber1);
                }
        );

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    // second window
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber1);
                    assertThat(batch.key.getWinStart().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis());
                    assertThat(batch.key.getWinEnd().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis() * 2);
                    assertThat(batch.value.getOrders()).hasSize(1);
                    assertThat(batch.value.getOrders().get(0).getOrderNumber()).isEqualTo(orderNumber2);
                }
        );
    }

    @Test
    public void shouldEmitWindowsContinuouslyWhileIgnoringGapsInBetween() {
        final Long orderNumber1 = 101L;
        final Long customerNumber1 = 6001L;

        final Long orderNumber2 = 102L;
        final Long customerNumber2 = 6002L;

        orderTopic.pipeInput(
                testOrderKey(orderNumber1),
                testOrderValue(orderNumber1, customerNumber1)
        );

        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);
        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);

        orderTopic.pipeInput(
                testOrderKey(orderNumber2),
                testOrderValue(orderNumber2, customerNumber2)
        );

        helper.getTestDriver().advanceWallClockTime(WINDOW_SIZE);

        final List<KeyValue<CustomerOrderBatchKey, CustomerOrderBatchValue>> customerOrderBatches =
                customerOrderBatchTopic.readKeyValuesToList();

        assertThat(customerOrderBatches).hasSize(2);

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    // first window
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber1);
                    assertThat(batch.key.getWinStart().toEpochMilli()).isEqualTo(0);
                    assertThat(batch.key.getWinEnd().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis());
                    assertThat(batch.value.getOrders()).hasSize(1);
                    assertThat(batch.value.getOrders().get(0).getOrderNumber()).isEqualTo(orderNumber1);
                }
        );

        assertThat(customerOrderBatches).satisfiesOnlyOnce(
                batch -> {
                    // second window
                    assertThat(batch.key.getCustomerNumber()).isEqualTo(customerNumber2);
                    assertThat(batch.key.getWinStart().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis() * 2);
                    assertThat(batch.key.getWinEnd().toEpochMilli()).isEqualTo(WINDOW_SIZE.toMillis() * 3);
                    assertThat(batch.value.getOrders()).hasSize(1);
                    assertThat(batch.value.getOrders().get(0).getOrderNumber()).isEqualTo(orderNumber2);
                }
        );
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("scheduledStreamsBuilder")
        StreamsBuilder scheduledStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setScheduledJobAppId("scheduled-job-topology-test");
            properties.setOrderTopic(ORDER_TOPIC);
            properties.setCustomerOrderBatchTopic(CUSTOMER_ORDER_BATCH);
            properties.setScheduleInterval(Duration.ofMinutes(5));

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
