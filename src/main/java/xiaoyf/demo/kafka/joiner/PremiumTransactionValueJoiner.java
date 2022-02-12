package xiaoyf.demo.kafka.joiner;

import demo.model.CustomerDetails;
import demo.model.CustomerOrder;
import demo.model.PremiumOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@Slf4j
public class PremiumTransactionValueJoiner implements ValueJoiner<CustomerOrder, CustomerDetails, PremiumOrder> {

    @Override
    public PremiumOrder apply(CustomerOrder customerOrder, CustomerDetails customerDetails) {
        log.info("PremiumTransactionValueJoiner joining...{}, {}", customerOrder, customerDetails);
        var activeCampaigns = customerDetails.getActiveCampaigns();

        if (Objects.isNull(activeCampaigns)) {
            return null;
        }

        if (!activeCampaigns.contains(customerOrder.getCampaign())) {
            return null;
        }

        return PremiumOrder.newBuilder()
                .setAmount(customerOrder.getAmount())
                .setCustomerNumber(customerOrder.getCustomerNumber())
                .setProductName(customerOrder.getProductName())
                .setOrderNumber(customerOrder.getOrderNumber())
                .setCampaign(customerOrder.getCampaign())
                .setName(customerDetails.getName())
                .setEmail(customerDetails.getEmail())
                .build();
    }
}
