package xiaoyf.demo.kafka.commons.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
public class LogAndContinueMapper<V, VR> implements ValueMapper<V, VR> {

    private final ValueMapper<V, VR> mapper;

    public LogAndContinueMapper(ValueMapper<V, VR> mapper) {
        this.mapper = mapper;
    }

    @Override
    public VR apply(V value) {
        try {
            return mapper.apply(value);
        } catch (RuntimeException re) {
            log.error("Error when applying mapper {}", mapper, re);
            return null;
        }
    }
}
