package xiaoyf.demo.kafka.helper.testhelper;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import xiaoyf.demo.kafka.helper.serde.AnySerde;

import java.util.List;
import java.util.Properties;

import static xiaoyf.demo.kafka.helper.serde.AvroUtils.MOCK_CONFIG;

public class ProcessorTestHelper<KI, VI, KO, VO> {
    private final MockProcessorContext<KO, VO> context;
    public ProcessorTestHelper() {

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, AnySerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AnySerde.class);

        context = new MockProcessorContext<>(config);
    }

    public ProcessorTestHelper<KI, VI, KO, VO> init(Processor<KI, VI, KO, VO> processor) {
        processor.init(context);
        return this;
    }

    public <K extends SpecificRecord, V extends SpecificRecord> ProcessorTestHelper<KI, VI, KO, VO> withStore(String storeName) {

        Serde<K> anyKeySerde = new AnySerde<>();
        anyKeySerde.configure(MOCK_CONFIG, true);

        Serde<V> anyValueSerde = new AnySerde<>();
        anyValueSerde.configure(MOCK_CONFIG, false);

        KeyValueStore<K, V>store = Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        anyKeySerde,
                        anyValueSerde)
                .withLoggingDisabled().build();

        store.init(context.getStateStoreContext(), store);
        context.addStateStore(store);
        return this;
    }

    public <K, V> KeyValueStore<K, V> store(String storeName) {
        return context.getStateStore(storeName);
    }

    public List<MockProcessorContext.CapturedForward<? extends KO, ? extends VO>> forwarded() {
        return context.forwarded();
    }

}
