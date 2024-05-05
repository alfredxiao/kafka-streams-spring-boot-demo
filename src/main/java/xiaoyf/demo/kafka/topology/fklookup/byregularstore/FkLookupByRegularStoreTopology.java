package xiaoyf.demo.kafka.topology.fklookup.byregularstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerStoreLookupProcessor;

/*
 Regular store uses its input topic as changelog topic, no matter store type is persistent or memory based.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FkLookupByRegularStoreTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;

    @Autowired
    @Qualifier("regularCustomerStoreBuilder")
    private final StoreBuilder<KeyValueStore<CustomerKey, CustomerValue>> customerStoreBuilder;
    private final Serde<CustomerKey> keySerde;
    private final Serde<CustomerValue> valueSerde;

    @Autowired
    void process(@Qualifier("fkLookupByRegularStoreStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);


        builder.addStateStore(customerStoreBuilder);
        builder.<CustomerKey, CustomerValue>stream(properties.getCustomerTopic())
                .process(
                        CustomerStoreProcessor::new,
                        Named.as("update-customer-store"),
                        CustomerStoreProcessor.CUSTOMER_STORE);

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .process(
                        CustomerStoreLookupProcessor::new,
                        Named.as("lookup-customer"),
                        CustomerStoreProcessor.CUSTOMER_STORE)
                .to(properties.getOrderEnrichedByRegularStoreTopic());
    }
}