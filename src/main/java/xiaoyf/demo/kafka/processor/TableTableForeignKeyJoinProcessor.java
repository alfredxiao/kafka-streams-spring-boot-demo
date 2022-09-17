package xiaoyf.demo.kafka.processor;


import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.filter.BigPurchaseFilter;
import xiaoyf.demo.kafka.joiner.PremiumOrderValueJoiner;
import xiaoyf.demo.kafka.mapper.PremiumOrderKeyMapper;

import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;

/**
 * TableTableForeignKeyJoinProcessor demonstrates a foreign key table-table join. Left topic is 'order', right
 * topic is 'customer'. We extract a customer identity from 'order' and then join with 'customer' to enhance the order
 * message, add filtering, end up having a 'premium-order' as result topic.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TableTableForeignKeyJoinProcessor {

    private final BigPurchaseFilter bigPurchase;
    private final PremiumOrderValueJoiner valueJoiner;
    private final PremiumOrderKeyMapper keyMapper;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("TableTableForeignKeyJoinProcessor use builder:" + builder);

        KTable<CustomerDetailsKey, CustomerDetails> customerDetails =
                builder.table(CUSTOMER_DETAIL_TOPIC, Consumed.as("CUST-DETAILS-TABLE"), Materialized.as("CUST-DETAIL-TABLE-STORE"));

        KStream<CustomerOrderKey, CustomerOrder> orderStream = builder.stream(CUSTOMER_ORDER_TOPIC, Consumed.as("CUSTOMER-ORDER-STREAM"));

        orderStream
                .filter(bigPurchase, Named.as("FILTER-BIG-PURCHASE"))
                .toTable(Named.as("CUST-ORDER-TABLE"), Materialized.as("CUST-ORDER-TABLE-STORE"))
                .join(customerDetails, this::extractCustomerKey, valueJoiner, Named.as("ORDER-JOINS-CUST-DETAIL"), Materialized.as("ORDER-JOINS-CUST-DETAIL-STORE"))
                .toStream(Named.as("TO-JOINED-ORDER"))
                .filter((key, value) -> value != null, Named.as("FILTER-ONLY-MATCHED"))
                .selectKey(keyMapper, Named.as("SELECT-PREMIUM-ORDER-KEY"))
                .to(PREMIUM_ORDER_TOPIC, Produced.as("TO-PREMIUM-ORDER"));
    }

    private CustomerDetailsKey extractCustomerKey(final CustomerOrder customerOrder) {
        return CustomerDetailsKey.newBuilder()
                .setCustomerNumber(customerOrder.getCustomerNumber())
                .build();
    }
}