package xiaoyf.demo.kafka.topology.idshortener.registry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

import static xiaoyf.demo.kafka.topology.idshortener.registry.ShortIdRegistryProcessor.LONG_ID_TO_SHORT_ID_STORE;

/*
  ShortIdRegistryTopology generates short id for long ids.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ShortIdRegistryTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;
    private final StoreBuilder<KeyValueStore<String, Integer>> longToShortIdStoreBuilder;
    private final ReverseKeyValueMapper reverseMapper;

    @Autowired
    void process(@Qualifier("shortIdRegistryStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        builder.addStateStore(longToShortIdStoreBuilder);

        // source topic: partition=1, key is the string id, value is any Integer (that is not null)
        builder.<String, Integer>stream(properties.getLongIdTopic())
                .process(ShortIdRegistryProcessor::new, LONG_ID_TO_SHORT_ID_STORE)
                .to(properties.getLongIdToShortIdTopic());

        // write to short to long mapping topic is to provide a reverse lookup, making it easier to
        // troubleshoot when needed.
        builder.<String, Integer>stream(properties.getLongIdToShortIdTopic())
                .map(reverseMapper)
                .to(properties.getShortIdToLongIdTopic());
    }
}