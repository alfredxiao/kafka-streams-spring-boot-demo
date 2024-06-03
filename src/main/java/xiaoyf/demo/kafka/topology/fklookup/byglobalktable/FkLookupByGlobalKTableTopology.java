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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerNumberExtractor;
import xiaoyf.demo.kafka.topology.fklookup.commons.OrderCustomerJoiner;

/*
  GlobalKTable uses its input topic as changelog, so it's better to be set as compacted topic.
  NOTE:
   1. GlobalKTable is NOT time-synchronised: When there are existing records on 'location' topic, they are read into
      GlobalKTable as first stage of stream processing before joining occurs. As a result, the following two scenarios
      yield different/inconsistent results
      - t0: start streaming application,
        t1: insert 'click'    k1:Click1
        t2: insert 'location' k1:Melbourne
        -> EMPTY join output
      - t0: insert 'click'    k1:Click1,
        t1: insert 'location' k1:Melbourne
        t2: start streaming application
        -> Click1:Melbourne joined
 */
@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="fk-lookup-by-global-ktable-app-enabled",
        havingValue = "true"
)
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
                builder.globalTable(properties.getCustomerTopic(), Materialized.as("customer-global-table"));

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .leftJoin(
                        customerTable,
                        customerNumberExtractor,
                        orderCustomerJoiner,
                        Named.as("order-joins-global-customer")
                )
                .to(properties.getOrderEnrichedByGlobalKTableTopic());
    }
}