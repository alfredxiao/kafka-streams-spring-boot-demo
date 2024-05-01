package xiaoyf.demo.kafka.commons.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

@Slf4j
public class LogAndContinueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, VR> {

    private final ValueMapperWithKey<K, V, VR> mapper;

    public LogAndContinueMapperWithKey(ValueMapperWithKey<K, V, VR> aMapper) {
        this.mapper = aMapper;
    }

    @Override
    public VR apply(K key, V value) {
        try {
            return mapper.apply(key, value);
        } catch (RuntimeException re) {
            log.error("Error when applying mapper-with-key {}", mapper, re);
            return null;
        }
    }
}
