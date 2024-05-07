package xiaoyf.demo.kafka.topology.idshortener.registry;

import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xiaoyf.demo.kafka.helper.testhelper.ProcessorTestHelper;
import xiaoyf.demo.kafka.helper.testhelper.StoreUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.topology.idshortener.registry.ShortIdRegistryProcessor.LONG_ID_TO_SHORT_ID_STORE;
import static xiaoyf.demo.kafka.topology.idshortener.registry.ShortIdRegistryProcessor.MAX;

public class ShortIdRegistryProcessorTest {

    private static final String ID1 = "id0011";
    private static final String ID2 = "id0022";
    private ShortIdRegistryProcessor processor;
    private ProcessorTestHelper<String, Integer, String, Integer> helper;

    @BeforeEach
    public void init() {
        processor = new ShortIdRegistryProcessor();
        helper = new ProcessorTestHelper<String, Integer, String, Integer>()
                .withStore(LONG_ID_TO_SHORT_ID_STORE)
                .init(processor);
    }
    
    private KeyValueStore<String, Integer> longIdToShortIdStore() {
        return helper.store(LONG_ID_TO_SHORT_ID_STORE);
    }

    @Test
    public void shouldCreateFirstEverShortId() {
        processor.process(new Record<>(ID1, -1, 1000L));

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(1);

        final var result = forwarded.get(0).record();
        assertThat(result.key()).isEqualTo(ID1);
        assertThat(result.value()).isEqualTo(1);

        Map<String, Integer> all = StoreUtils.getAll(longIdToShortIdStore());
        assertThat(all).isEqualTo(Map.of(
                ID1, 1,
                MAX, 1
        ));
    }

    @Test
    public void shouldReturnExistingShortId() {
        final int existing = 10;
        longIdToShortIdStore().put(ID1, existing);
        processor.process(new Record<>(ID1, -1, 1000L));

        final var forwarded = helper.forwarded();

        assertThat(forwarded).hasSize(0);

        Map<String, Integer> all = StoreUtils.getAll(longIdToShortIdStore());
        assertThat(all).isEqualTo(Map.of(
                ID1, existing
        ));
    }

    @Test
    public void shouldIncreaseShortIdForADifferentLongId() {
        final int max = 10;
        longIdToShortIdStore().put(ID1, max);
        longIdToShortIdStore().put(MAX, max);
        processor.process(new Record<>(ID2, -1, 1000L));

        final var forwarded = helper.forwarded();
        assertThat(forwarded).hasSize(1);

        final var result = forwarded.get(0).record();
        assertThat(result.key()).isEqualTo(ID2);
        assertThat(result.value()).isEqualTo(max + 1);

        Map<String, Integer> all = StoreUtils.getAll(longIdToShortIdStore());
        assertThat(all).isEqualTo(Map.of(
                ID1, max,
                ID2, max + 1,
                MAX, max + 1
        ));
    }

}
