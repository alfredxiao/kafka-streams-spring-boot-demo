package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderValue;
import demo.model.OrderKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DedupeTopologyConfiguration {

    @Bean
    public StoreBuilder<KeyValueStore<OrderKey, OrderValue>> orderStoreBuilder(
            Serde<OrderKey> keySerde,
            Serde<OrderValue> valueSerde) {

        // NOTE: in memory store is also backed by changelog topic.
        // see https://stackoverflow.com/questions/57055684/kafka-persistent-statestore-vs-in-memory-state-store
        // points: 1. both types are backed by changelog topic;
        // 2. whole data must fit in memory when memory store is used; 3. persistent store has faster recovery time
        // also note: If .withLoggingDisabled() is used on the builder, store will only include records created after
        //            app has started, which means it does not recover from a topic.
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(StoreBasedDedupeProcessor.ORDER_STORE),
//                Stores.persistentKeyValueStore(StoreBasedDedupeProcessor.ORDER_STORE),
                keySerde,
                valueSerde);
    }
}
