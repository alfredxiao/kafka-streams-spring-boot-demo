package xiaoyf.demo.kafka.helper.testhelper;

import lombok.experimental.UtilityClass;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
public class StoreUtils {
    public <K,V> Map<K, V> getAll(ReadOnlyKeyValueStore<K, V> store) {

        Map<K, V> result = new HashMap<>();

        KeyValueIterator<K, V> iter = store.all();
        try {
            while (iter.hasNext()) {
                KeyValue<K, V> entry = iter.next();
                if (entry.value != null) {
                    result.put(entry.key, entry.value);
                }
            }
        } finally {
            iter.close();
        }

        return result;
    }

}
