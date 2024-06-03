package xiaoyf.demo.kafka.topology.fklookup.byregularstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * CustomerStoreProcessor puts customer records into a store
 */
@Slf4j
public class CustomerStoreProcessor implements Processor<CustomerKey, CustomerValue, CustomerKey, CustomerValue> {
    public static final String CUSTOMER_STORE = "customer-store";
    private KeyValueStore<CustomerKey, CustomerValue> store;

    @Override
    public void init(ProcessorContext<CustomerKey, CustomerValue> context) {
        this.store = context.getStateStore(CUSTOMER_STORE);
    }

    @Override
    public void process(Record<CustomerKey, CustomerValue> record) {
        log.info("storing customer record: {}", record);
        store.put(record.key(), record.value());
    }

}
