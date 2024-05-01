package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderValue;
import demo.model.OrderKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;

import static xiaoyf.demo.kafka.topology.dedupe.StoreBasedDedupeProcessor.ORDER_STORE;

@Component
@RequiredArgsConstructor
@Slf4j
public class DedupeTopology {
    private final DemoProperties properties;
    private final StoreBuilder<KeyValueStore<OrderKey, OrderValue>> orderStoreBuilder;

    @Autowired(required = false)
    private GitProperties gitProperties;

    @Autowired
    void process(@Qualifier("dedupeStreamsBuilder") StreamsBuilder builder) {
        log.info("DedupeTopology Processor Config {}", properties);
        log.info("git commit: {}", gitProperties == null ? "NULL" : gitProperties.getShortCommitId());

        builder.addStateStore(orderStoreBuilder);

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .process(
                        StoreBasedDedupeProcessor::new,
                        Named.as("dedupe-processor"),
                        ORDER_STORE)
                .to(properties.getOrderDedupedTopic());
    }
}

// todo: persistent and in memory