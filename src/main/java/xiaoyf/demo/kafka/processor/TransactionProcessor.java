package xiaoyf.demo.kafka.processor;


import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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

        KTable<CustomerDetailsKey, CustomerDetails> customerDetails = builder.table(CUSTOMER_DETAILS_TOPIC, Materialized.as("CUST-DETAILS-TABLE"));

        KStream<CustomerOrderKey, CustomerOrder> transactions = builder.stream(CUSTOMER_ORDER_TOPIC);

        transactions
                .filter(bigPurchase)
                .toTable(Materialized.as("CUST-ORDER-TABLE"))
                .join(customerDetails, this::extractForeignKey, valueJoiner, Materialized.as("ORDER-JOINS-CUST-DETAILS"))
                .toStream()
                .filter((key, value) -> value != null)
                .selectKey(keyMapper)
                .to(PREMIUM_ORDER_TOPIC);
    }

    private CustomerDetailsKey extractForeignKey(final CustomerOrder customerOrder) {
        return CustomerDetailsKey.newBuilder()
                .setCustomerNumber(customerOrder.getCustomerNumber())
                .build();
    }
}