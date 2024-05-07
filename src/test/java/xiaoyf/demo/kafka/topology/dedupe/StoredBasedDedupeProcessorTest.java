package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderValue;
import demo.model.OrderKey;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xiaoyf.demo.kafka.helper.testhelper.ProcessorTestHelper;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.topology.dedupe.StoreBasedDedupeProcessor.ORDER_STORE;

public class StoredBasedDedupeProcessorTest {

    private StoreBasedDedupeProcessor processor;
    private ProcessorTestHelper<OrderKey, OrderValue, OrderKey, OrderValue> helper;

    @BeforeEach
    public void init() {
        processor = new StoreBasedDedupeProcessor();
        helper = new ProcessorTestHelper<OrderKey, OrderValue, OrderKey, OrderValue>()
                .withStore(ORDER_STORE)
                .init(processor);
    }

    @Test
    public void shouldPutInStoreForTheFirstTime() {
        Record<OrderKey, OrderValue> record = new Record<>(
                testOrderKey(),
                testOrderValue(),
                1L
        );
        processor.process(record);

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(1);
        assertThat(helper.store(ORDER_STORE).get(record.key())).isNotNull();
    }

    @Test
    public void shouldDetectDuplicateAndNotForwardAgain() {
        Record<OrderKey, OrderValue> record1 = new Record<>(
                testOrderKey(),
                testOrderValue(),
                1L
        );
        Record<OrderKey, OrderValue> record2 = new Record<>(
                testOrderKey(),
                testOrderValue(),
                2L
        );
        processor.process(record1);
        processor.process(record2);

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(1);
    }

    @Test
    public void shouldForwardChangeAndUpdateStore() {
        Record<OrderKey, OrderValue> record1 = new Record<>(
                testOrderKey(),
                testOrderValue(),
                1L
        );

        OrderValue orderUpdated = OrderValue.newBuilder(testOrderValue())
                .setProductName("NewProduct")
                .build();
        Record<OrderKey, OrderValue> record2 = new Record<>(
                testOrderKey(),
                orderUpdated,
                2L
        );
        processor.process(record1);
        processor.process(record2);

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(2);
        assertThat(helper.store(ORDER_STORE).get(record2.key())).isEqualTo(orderUpdated);
    }

    @Test
    public void shouldForwardAndStoreBecauseNewKey() {
        Record<OrderKey, OrderValue> record1 = new Record<>(
                testOrderKey(),
                testOrderValue(),
                1L
        );
        Record<OrderKey, OrderValue> record2 = new Record<>(
                OrderKey.newBuilder(testOrderKey())
                        .setOrderNumber(9999)
                        .build(),
                testOrderValue(),
                2L
        );
        processor.process(record1);
        processor.process(record2);

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(2);
        assertThat(helper.store(ORDER_STORE).approximateNumEntries()).isEqualTo(2);
    }

}
