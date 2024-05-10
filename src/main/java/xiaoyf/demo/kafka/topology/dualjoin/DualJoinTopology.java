package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

import static xiaoyf.demo.kafka.topology.dualjoin.PreferenceJoiningProcessor.PREFERENCE_STORE;
import static xiaoyf.demo.kafka.topology.dualjoin.ContactJoiningProcessor.CONTACT_STORE;

/*
 Dual Joining demos custom way to do stream-stream joining where either side of changes can trigger
 and emit an output. This is similar to a table-table join (with foreign key)
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DualJoinTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;
    @Autowired
    @Qualifier("contactStoreBuilder")
    private final StoreBuilder<KeyValueStore<CustomerKey, ContactValue>> contactStoreBuilder;

    @Autowired
    @Qualifier("preferenceStoreBuilder")
    private final StoreBuilder<KeyValueStore<PreferenceKey, PreferenceValue>> preferenceStoreBuilder;
    private final Serde<PreferenceKey> preferenceKeySerde;

    @Autowired
    void process(@Qualifier("dualJoinStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        builder.addStateStore(contactStoreBuilder);
        builder.addStateStore(preferenceStoreBuilder);

        builder.<PreferenceKey, PreferenceValue>stream(properties.getPreferenceTopic())
                .process(() -> new RekeyProcessor(preferenceKeySerde, properties))
                .repartition()
                .peek((k,v) -> log.info("##!!3 {}/{}", k,v))
                .process(() -> new StoreAndForwardProcessor<>(PREFERENCE_STORE), PREFERENCE_STORE)
                .peek((k,v) -> log.info("##!!4 {}/{}", k,v))
                .process(ContactJoiningProcessor::new, CONTACT_STORE)
                .peek((k,v) -> log.info("##!!5 {}/{}", k,v))
                .to(properties.getEnrichedPreferenceTopic());

        builder.<CustomerKey, ContactValue>stream(properties.getContactTopic())
                .process(() -> new StoreAndForwardProcessor<>(CONTACT_STORE), CONTACT_STORE)
                .peek((k,v) -> log.info("##!!1 {}/{}", k,v))
                .process(PreferenceJoiningProcessor::new, PREFERENCE_STORE)
                .peek((k,v) -> log.info("##!!2 {}/{}", k,v))
                .to(properties.getEnrichedPreferenceTopic());

    }
}