package xiaoyf.demo.kafka.topology.dualjoin;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class  StorePutProcessor<K, V> implements Processor<K, V, K, V> {

    private final String storeName;
    private KeyValueStore<K, V> store;

    public StorePutProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.store = context.getStateStore(this.storeName);
    }

    @Override
    public void process(Record<K, V> record) {
        this.store.put(record.key(), record.value());
    }
}
