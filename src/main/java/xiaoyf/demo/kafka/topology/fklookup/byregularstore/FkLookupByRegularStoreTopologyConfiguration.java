package xiaoyf.demo.kafka.topology.fklookup.byregularstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(
        prefix="demo-streams",
        name="fk-lookup-by-regular-store-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class FkLookupByRegularStoreTopologyConfiguration {

    @Bean("regularCustomerStoreBuilder")
    public StoreBuilder<KeyValueStore<CustomerKey, CustomerValue>> regularCustomerStoreBuilder(
            Serde<CustomerKey> keySerde,
            Serde<CustomerValue> valueSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(CustomerStoreProcessor.CUSTOMER_STORE),
                keySerde,
                valueSerde);
    }

}
