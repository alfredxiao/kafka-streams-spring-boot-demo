package xiaoyf.demo.kafka.topology.fklookup.byglobalstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.commons.processor.GlobalStateStoreLoadingProcessor;
import xiaoyf.demo.kafka.config.DemoProperties;

import static xiaoyf.demo.kafka.topology.fklookup.byglobalstore.FkLookupProcessor.CUSTOMER_STORE;

/*
 Global store uses its input topic as changelog topic, no matter store type is persistent or memory based.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FkLookupByGlobalStoreTopology {
    private final DemoProperties properties;
    private final StoreBuilder<KeyValueStore<CustomerKey, CustomerValue>> customerStoreBuilder;
    private final Serde<CustomerKey> keySerde;
    private final Serde<CustomerValue> valueSerde;

    @Autowired(required = false)
    private GitProperties gitProperties;

    @Autowired
    void process(@Qualifier("fkLookupByGlobalStoreStreamsBuilder") StreamsBuilder builder) {
        log.info("FkLookupByGlobalStoreTopology Processor Config {}", properties);
        log.info("git commit: {}", gitProperties == null ? "NULL" : gitProperties.getShortCommitId());

        builder.addGlobalStore(customerStoreBuilder,
                properties.getCustomerTopic(),
                Consumed.with(keySerde, valueSerde),
                () -> new GlobalStateStoreLoadingProcessor<>(CUSTOMER_STORE)
        );

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .process(
                        FkLookupProcessor::new,
                        Named.as("lookup-customer"))
                .to(properties.getOrderEnrichedTopic());
    }
}