package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.CustomerKey;
import demo.model.EnrichedPreferenceValue;
import demo.model.PreferenceValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ContactJoiningProcessor implements Processor<CustomerKey, PreferenceValue, CustomerKey, EnrichedPreferenceValue> {

    public static final String CONTACT_STORE = "contact-store";

    @Override
    public void init(ProcessorContext<CustomerKey, EnrichedPreferenceValue> context) {
        Processor.super.init(context);
    }

    @Override
    public void process(Record<CustomerKey, PreferenceValue> record) {

    }
}
