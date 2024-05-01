package xiaoyf.demo.kafka.commons.wrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Predicate;

@Slf4j
public class LogAndContinueFilter<K, V> implements Predicate<K, V> {

    private final Predicate<K, V> filter;

    public LogAndContinueFilter(Predicate<K, V> aFilter) {
        this.filter = aFilter;
    }

    @Override
    public boolean test(K key, V value) {
        try {
            return filter.test(key, value);
        } catch (RuntimeException re) {
            log.error("Error when applying filter {}", filter, re);
            return false;
        }
    }
}
