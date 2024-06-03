package xiaoyf.demo.kafka.topology.idshortener.registry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static xiaoyf.demo.kafka.topology.idshortener.registry.ShortIdRegistryProcessor.LONG_ID_TO_SHORT_ID_STORE;

@Configuration
@ConditionalOnProperty(
        prefix="demo-streams",
        name="short-id-registry-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class ShortIdRegistryTopologyConfiguration {

    @Bean("longToShortIdStoreBuilder")
    public StoreBuilder<KeyValueStore<String, Integer>> longToShortIdStoreBuilder() {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(LONG_ID_TO_SHORT_ID_STORE),
                Serdes.String(),
                Serdes.Integer());
    }

}
