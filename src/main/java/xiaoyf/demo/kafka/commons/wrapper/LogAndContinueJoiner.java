package xiaoyf.demo.kafka.commons.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

@Slf4j
public class LogAndContinueJoiner<V1, V2, VR> implements ValueJoiner<V1, V2, VR> {

    private final ValueJoiner<V1, V2, VR> joiner;

    public LogAndContinueJoiner(ValueJoiner<V1, V2, VR> aJoiner) {
        this.joiner = aJoiner;
    }

    @Override
    public VR apply(V1 value1, V2 value2) {
        try {
            return joiner.apply(value1, value2);
        } catch (RuntimeException re) {
            log.error("Error when applying joiner {}", joiner, re);
            return null;
        }
    }
}
