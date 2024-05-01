package xiaoyf.demo.kafka.commons.wrapper;

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

public class Wrappers {

    public static <V1, V2, VR> ValueJoiner<V1, V2, VR> wrap(ValueJoiner<V1, V2, VR> joiner) {
        return new LogAndContinueJoiner<>(joiner);
    }

    public static <V, VR> ValueMapper<V, VR> wrap(ValueMapper<V, VR> mapper) {
        return new LogAndContinueMapper<>(mapper);
    }

    public static <K, V, VR> ValueMapperWithKey<K, V, VR> wrap(ValueMapperWithKey<K, V, VR> mapper) {
        return new LogAndContinueMapperWithKey<>(mapper);
    }

    public static <K, V> Predicate<K, V> wrap(Predicate<K, V> filter) {
        return new LogAndContinueFilter<>(filter);
    }
}
