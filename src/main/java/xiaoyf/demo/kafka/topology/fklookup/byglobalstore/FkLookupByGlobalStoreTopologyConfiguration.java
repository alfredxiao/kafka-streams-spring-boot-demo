package xiaoyf.demo.kafka.topology.fklookup.byglobalstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerStoreLookupProcessor;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class FkLookupByGlobalStoreTopologyConfiguration {

    @Bean("customerStoreBuilder")
    public StoreBuilder<KeyValueStore<CustomerKey, CustomerValue>> customerStoreBuilder(
            Serde<CustomerKey> keySerde,
            Serde<CustomerValue> valueSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(CustomerStoreLookupProcessor.CUSTOMER_STORE),
                keySerde,
                valueSerde);
    }

}
