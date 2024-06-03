package xiaoyf.demo.kafka.topology.dualjoin.bystore;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static xiaoyf.demo.kafka.topology.dualjoin.bystore.PreferenceJoiningProcessor.PREFERENCE_STORE;
import static xiaoyf.demo.kafka.topology.dualjoin.bystore.ContactJoiningProcessor.CONTACT_STORE;

@Configuration
@ConditionalOnProperty(
        prefix="demo-streams",
        name="dual-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class DualJoinTopologyConfiguration {

    @Bean("contactStoreBuilder")
    public StoreBuilder<KeyValueStore<CustomerKey, ContactValue>> contactStoreBuilder(
            Serde<CustomerKey> keySerde,
            Serde<ContactValue> valueSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(CONTACT_STORE),
                keySerde,
                valueSerde);
    }

    @Bean("preferenceStoreBuilder")
    public StoreBuilder<KeyValueStore<PreferenceKey, PreferenceValue>> preferenceStoreBuilder(
            Serde<PreferenceKey> keySerde,
            Serde<PreferenceValue> valueSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(PREFERENCE_STORE),
                keySerde,
                valueSerde);
    }
}
