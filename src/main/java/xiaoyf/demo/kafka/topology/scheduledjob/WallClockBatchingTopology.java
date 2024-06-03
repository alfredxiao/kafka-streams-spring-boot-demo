package xiaoyf.demo.kafka.topology.scheduledjob;

import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
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
        name="scheduled-job-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class WallClockBatchingTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;

    @Autowired
    void process(@Qualifier("scheduledStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .process(
                        () -> new WallClockBatchingProcessor(properties.getScheduleInterval()),
                        Named.as("wall-clock-batching-processor"))
                .to(properties.getCustomerOrderBatchTopic());
    }
}