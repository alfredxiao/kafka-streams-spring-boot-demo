package xiaoyf.demo.kafka.joiner;

import demo.model.CustomerDetails;
import demo.model.CustomerOrder;
import demo.model.PremiumOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * PremiumOrderValueJoiner serves to purposes here: 1. joins order and customer details to form a new object - premium
 * order; 2. filters only matched customers, otherwise return null.
 */
@Component
@Slf4j
public class PremiumOrderValueJoiner implements ValueJoiner<CustomerOrder, CustomerDetails, PremiumOrder> {

    @Override
    public PremiumOrder apply(CustomerOrder customerOrder, CustomerDetails customerDetails) {
        log.info("PremiumOrderValueJoiner joining...{}, {}", customerOrder, customerDetails);
        var activeCampaigns = customerDetails.getActiveCampaigns();

        if (Objects.isNull(activeCampaigns)) {
            log.info("customer joins no campaigns, ignore order");
            return null;
        }

        if (!activeCampaigns.contains(customerOrder.getCampaign())) {
            log.info("order not matching customer's campaigns, ignore order");
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
