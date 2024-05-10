package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.CustomerKey;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;

@Component
@RequiredArgsConstructor
public class RekeyProcessor implements Processor<PreferenceKey, PreferenceValue, CustomerKey, PreferenceValue> {
    private ProcessorContext<CustomerKey, PreferenceValue> context;
    private final Serde<PreferenceKey> preferenceKeySerde;
    private final DemoProperties properties;

    @Override
    public void init(ProcessorContext<CustomerKey, PreferenceValue> context) {
        this.context = context;
    }

    @Override
    public void process(Record<PreferenceKey, PreferenceValue> record) {
        PreferenceKeyedByAddressNumber mapper = new PreferenceKeyedByAddressNumber();
        KeyValue<CustomerKey, PreferenceValue> rekeyed = mapper.apply(record.key(), record.value());

        byte[] preferenceKeyBytes = preferenceKeySerde.serializer().serialize(properties.getPreferenceTopic(), record.key());

        this.context.forward(new Record<>(
                rekeyed.key,
                rekeyed.value,
                record.timestamp(),
                record.headers().add("preferenceKey", preferenceKeyBytes)
        ));
    }
}
