package xiaoyf.demo.kafka.topology.dualjoin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class StoreAndForwardProcessor<K, V> implements Processor<K, V, K, V> {

    private final String storeName;
    private KeyValueStore<K, V> store;
    private ProcessorContext<K, V> context;

    public StoreAndForwardProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext<K, V> context) {
        this.store = context.getStateStore(this.storeName);
        this.context = context;
    }

    @Override
    public void process(Record<K, V> record) {
        this.store.put(record.key(), record.value());
        this.context.forward(record);
    }
}
