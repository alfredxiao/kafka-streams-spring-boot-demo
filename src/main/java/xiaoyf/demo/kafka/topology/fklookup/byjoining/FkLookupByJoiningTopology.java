package xiaoyf.demo.kafka.topology.fklookup.byjoining;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerNumberExtractor;
import xiaoyf.demo.kafka.topology.fklookup.commons.OrderCustomerJoiner;

@Component
@RequiredArgsConstructor
@Slf4j
public class FkLookupByJoiningTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;

    private final OrderCustomerJoiner orderCustomerJoiner;
    private final CustomerNumberExtractor customerNumberExtractor;

    @Autowired
    void process(@Qualifier("fkLookupByJoiningStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        final KTable<CustomerKey, CustomerValue> customerTable =
                builder.<CustomerKey, CustomerValue>stream(properties.getCustomerTopic())
                        .toTable(Named.as("customer-table"));

        builder.<OrderKey, OrderValue>stream(properties.getOrderTopic())
                .selectKey(customerNumberExtractor)
                .leftJoin(
                        customerTable,
                        orderCustomerJoiner,
                        Joined.as("order-joins-customer")
                )
                .selectKey((k, v) -> OrderKey.newBuilder().setOrderNumber(v.getOrderNumber()).build())
                .to(properties.getOrderEnrichedByJoiningTopic());
    }
}