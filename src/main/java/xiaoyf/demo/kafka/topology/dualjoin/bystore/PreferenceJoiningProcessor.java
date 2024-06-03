package xiaoyf.demo.kafka.topology.dualjoin.bystore;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.EnrichedPreferenceValue;
import demo.model.PreferenceValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Slf4j
public class PreferenceJoiningProcessor implements Processor<CustomerKey, ContactValue, CustomerKey, EnrichedPreferenceValue> {

    public static final String PREFERENCE_STORE = "preference-store";
    private ProcessorContext<CustomerKey, EnrichedPreferenceValue> context;
    private ReadOnlyKeyValueStore<CustomerKey, PreferenceValue> preferenceStore;

    @Override
    public void init(ProcessorContext<CustomerKey, EnrichedPreferenceValue> context) {
        this.preferenceStore = context.getStateStore(PREFERENCE_STORE);
        this.context = context;
    }

    @Override
    public void process(Record<CustomerKey, ContactValue> record) {
        final CustomerKey customerKey = record.key();
        final ContactValue contactValue = record.value();
        final PreferenceValue preferenceValue = preferenceStore.get(customerKey);
        if (preferenceValue == null) {
            log.info("preference not found by customerKey:{}", customerKey);
            return;
        }

        EnrichedPreferenceValue enriched = EnrichedPreferenceValue.newBuilder()
                .setCustomerNumber(preferenceValue.getCustomerNumber())
                .setPreferredComms(preferenceValue.getPreferredComms())
                .setContact(contactValue)
                .build();

        this.context.forward(new Record<>(
                customerKey,
                enriched,
                record.timestamp()
        ));
    }
}
