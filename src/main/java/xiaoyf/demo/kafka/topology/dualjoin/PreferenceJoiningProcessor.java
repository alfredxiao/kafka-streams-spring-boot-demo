package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.EnrichedPreferenceValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class PreferenceJoiningProcessor implements Processor<CustomerKey, ContactValue, CustomerKey, EnrichedPreferenceValue> {

    public static final String PREFERENCE_STORE = "preference-store";

    @Override
    public void init(ProcessorContext<CustomerKey, EnrichedPreferenceValue> context) {
        Processor.super.init(context);
    }

    @Override
    public void process(Record<CustomerKey, ContactValue> record) {
        // lookup preference by customerKey
    }
}
