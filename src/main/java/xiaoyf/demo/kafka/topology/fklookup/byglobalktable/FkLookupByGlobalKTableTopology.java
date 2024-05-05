package xiaoyf.demo.kafka.topology.fklookup.byglobalktable;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

/*
  GlobalKTable uses its input topic as changelog, so it's better to be set as compacted topic.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FkLookupByGlobalKTableTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;
    private final OrderCustomerJoiner orderCustomerJoiner;
    private final CustomerNumberExtractor customerNumberExtractor;

    @Autowired
    void process(@Qualifier("fkLookupByGlobalKTableStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        GlobalKTable<CustomerKey, CustomerValue> customerTable =
                builder.globalTable(properties.getCustomerTopic(), Materialized.as("customer-table"));

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .leftJoin(
                        customerTable,
                        customerNumberExtractor,
                        orderCustomerJoiner,
                        Named.as("order-joins-customer")
                )
                .to(properties.getOrderEnrichedByGlobalKTableTopic());
    }
}