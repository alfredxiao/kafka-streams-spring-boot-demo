package xiaoyf.demo.kafka.processor;


import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.filter.BigCampaignPurchase;
import xiaoyf.demo.kafka.joiner.PremiumTransactionValueJoiner;
import xiaoyf.demo.kafka.mapper.PremiumOrderKeyMapper;

import static xiaoyf.demo.kafka.helper.Const.*;

@Component
@RequiredArgsConstructor
public class TransactionProcessor {

    private final BigCampaignPurchase bigPurchase;

    private final PremiumTransactionValueJoiner valueJoiner;

    private final PremiumOrderKeyMapper keyMapper;

    @Autowired
    public void process(StreamsBuilder builder) {
        System.out.println("==== process");

        KTable<CustomerDetailsKey, CustomerDetails> customerDetails =
                builder.table(CUSTOMER_DETAILS_TOPIC, Consumed.as("CUST-DETAILS-TABLE"), Materialized.as("CUST-DETAILS-TABLE-STORE"));

        KStream<CustomerOrderKey, CustomerOrder> transactions = builder.stream(CUSTOMER_ORDER_TOPIC, Consumed.as("CUSTOMER-ORDER-STREAM"));

        transactions
                .filter(bigPurchase, Named.as("FILTER-BIG-PURCHASE"))
                .toTable(Named.as("CUST-ORDER-TABLE"), Materialized.as("CUST-ORDER-TABLE-STORE"))
                .join(customerDetails, this::extractForeignKey, valueJoiner, Named.as("ORDER-JOINS-CUST-DETAILS"), Materialized.as("ORDER-JOINS-CUST-DETAILS-STORE"))
                .toStream(Named.as("TO-JOINED-ORDER"))
                .filter((key, value) -> value != null, Named.as("FILTER-ONLY-MATCHED"))
                .selectKey(keyMapper, Named.as("SELECT-PREMIUM-ORDER-KEY"))
                .to(PREMIUM_ORDER_TOPIC, Produced.as("TO-PREMIUM-ORDER"));
    }

    private CustomerDetailsKey extractForeignKey(final CustomerOrder customerOrder) {
        return CustomerDetailsKey.newBuilder()
                .setCustomerNumber(customerOrder.getCustomerNumber())
                .build();
    }
}