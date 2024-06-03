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
public class ContactJoiningProcessor implements Processor<CustomerKey, PreferenceValue, CustomerKey, EnrichedPreferenceValue> {

    public static final String CONTACT_STORE = "contact-store";
    private ProcessorContext<CustomerKey, EnrichedPreferenceValue> context;
    private ReadOnlyKeyValueStore<CustomerKey, ContactValue> contactStore;

    @Override
    public void init(ProcessorContext<CustomerKey, EnrichedPreferenceValue> context) {
        contactStore = context.getStateStore(CONTACT_STORE);
        this.context = context;
    }

    @Override
    public void process(Record<CustomerKey, PreferenceValue> record) {
        final CustomerKey customerKey = record.key();
        final PreferenceValue preferenceValue = record.value();
        final ContactValue contactValue = contactStore.get(customerKey);
        if (contactValue == null) {
            log.info("contact not found by customerKey:{}", customerKey);
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
