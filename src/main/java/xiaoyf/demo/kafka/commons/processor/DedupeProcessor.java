package xiaoyf.demo.kafka.commons.processor;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

public class DedupeProcessor<K, V>  implements FixedKeyProcessor<K, V, V> {
    private FixedKeyProcessorContext<K, V> context;
    private final String storeName;

    public DedupeProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, V> localContext) {
        this.context = localContext;
    }

    @Override
    public void process(FixedKeyRecord<K, V> record) {
        KeyValueStore<K, V> store = context.getStateStore(storeName);
        K key = record.key();
        V value = record.value();
        V existing = store.get(record.key());

        if (!ProcessorUtils.isDuplicated(existing, value)) {
            store.put(key, value);
            context.forward(record);
        }
    }

}
