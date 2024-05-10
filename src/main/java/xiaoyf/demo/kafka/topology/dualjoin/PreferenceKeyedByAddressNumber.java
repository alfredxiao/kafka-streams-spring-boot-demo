package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class PreferenceKeyedByAddressNumber implements
        KeyValueMapper<PreferenceKey, PreferenceValue, KeyValue<CustomerKey, PreferenceValue>> {
    @Override
    public KeyValue<CustomerKey, PreferenceValue> apply(PreferenceKey key, PreferenceValue value) {
        return KeyValue.pair(
                CustomerKey.newBuilder().setCustomerNumber(value.getCustomerNumber()).build(),
                value
        );
    }
}
