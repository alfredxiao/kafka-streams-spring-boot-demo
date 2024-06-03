package xiaoyf.demo.kafka.topology.dedupe;

import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

import static xiaoyf.demo.kafka.topology.dedupe.StoreBasedDedupeProcessor.ORDER_STORE;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="dedupe-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class DedupeTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;
    private final StoreBuilder<KeyValueStore<OrderKey, OrderValue>> orderStoreBuilder;

    @Autowired
    void process(@Qualifier("dedupeStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        builder.addStateStore(orderStoreBuilder);

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .process(
                        StoreBasedDedupeProcessor::new,
                        Named.as("dedupe-processor"),
                        ORDER_STORE)
                .to(properties.getOrderDedupedTopic());
    }
}