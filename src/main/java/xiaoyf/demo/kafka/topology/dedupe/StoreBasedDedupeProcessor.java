package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderValue;
import demo.model.OrderKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import xiaoyf.demo.kafka.commons.processor.ProcessorUtils;

@Slf4j
public class StoreBasedDedupeProcessor
        implements Processor<OrderKey, OrderValue, OrderKey, OrderValue> {

    public static final String ORDER_STORE = "order-store";
    private ProcessorContext<OrderKey, OrderValue> context;
    protected KeyValueStore<OrderKey, OrderValue> store;

    @Override
    public void init(ProcessorContext<OrderKey, OrderValue> context) {
        this.context = context;
        this.store = context.getStateStore(ORDER_STORE);
    }

    @Override
    public void process(Record<OrderKey, OrderValue> record) {
        final OrderKey key = record.key();
        final OrderValue value = record.value();
        final OrderValue existing = store.get(record.key());

        if (!ProcessorUtils.isDuplicated(existing, value)) {
            store.put(key, value);
            context.forward(record);
            return;
        }

        log.info("duplicate record detected, k={}, v={}", key, value);
    }
}
