package xiaoyf.demo.kafka.topology.idshortener.registry;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class ReverseKeyValueMapper implements KeyValueMapper<String, Integer, KeyValue<Integer, String>> {
    @Override
    public KeyValue<Integer, String> apply(String key, Integer value) {
        if (value == null) {
            return null;
        }

        return KeyValue.pair(value, key);
    }
}
