package xiaoyf.demo.kafka.joiner;

import demo.model.CustomerValue;
import demo.model.OrderValue;
import demo.model.PremiumOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * PremiumOrderValueJoiner serves to purposes here: 1. joins order and customer details to form a new object - premium
 * order; 2. filters only matched customers, otherwise return null.
 */
@Component
@Slf4j
public class PremiumOrderValueJoiner implements ValueJoiner<OrderValue, CustomerValue, PremiumOrder> {

    @Override
    public PremiumOrder apply(OrderValue OrderValue, CustomerValue CustomerValue) {
        log.info("PremiumOrderValueJoiner joining...{}, {}", OrderValue, CustomerValue);
        var activeCampaigns = CustomerValue.getActiveCampaigns();

        if (Objects.isNull(activeCampaigns)) {
            log.info("customer joins no campaigns, ignore order");
            return null;
        }

        if (!activeCampaigns.contains(OrderValue.getCampaign())) {
            log.info("order not matching customer's campaigns, ignore order");
            return null;
        }

        return PremiumOrder.newBuilder()
                .setAmount(BigDecimal.valueOf(OrderValue.getQuantity()))
                .setCustomerNumber(OrderValue.getCustomerNumber())
                .setProductName(OrderValue.getProductName())
                .setOrderNumber(OrderValue.getOrderNumber())
                .setCampaign(OrderValue.getCampaign())
                .setName(CustomerValue.getName())
                .setEmail(CustomerValue.getEmail())
                .build();
    }
}
