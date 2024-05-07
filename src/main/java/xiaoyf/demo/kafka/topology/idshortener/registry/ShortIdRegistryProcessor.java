package xiaoyf.demo.kafka.topology.idshortener.registry;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class ShortIdRegistryProcessor implements Processor<String, Integer, String, Integer> {

    public static final String LONG_ID_TO_SHORT_ID_STORE = "long-id-to-short-id-store";
    public static final String MAX = "MAX";
    private ProcessorContext<String, Integer> context;
    private KeyValueStore<String, Integer> store;

    @Override
    public void init(ProcessorContext<String, Integer> context) {
        this.context = context;
        this.store = context.getStateStore(LONG_ID_TO_SHORT_ID_STORE);
    }

    @Override
    public void process(Record<String, Integer> record) {
        final String longId = record.key();
        Integer shortId = this.store.get(longId);

        if (shortId != null) {
            // nothing to do as already shortened
            return;
        }

        Integer maxId = this.store.get(MAX);
        if (maxId == null) {
            maxId = 1;
            shortId = 1;
        } else {
            maxId = maxId + 1;
            shortId = maxId;
        }

        store.put(MAX, maxId);
        store.put(longId, shortId);

        context.forward(new Record<>(longId, shortId, record.timestamp()));
    }
}
