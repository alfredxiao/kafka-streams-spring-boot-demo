package xiaoyf.demo.kafka.filter;

import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Objects;

@Component
public class BigCampaignPurchase implements Predicate<CustomerOrderKey, CustomerOrder> {
    private static final BigDecimal PREMIUM_STANDARD = new BigDecimal("1000.00");

    @Override
    public boolean test(CustomerOrderKey key, CustomerOrder value) {
        return Objects.nonNull(value.getCampaign())
                && value.getAmount().compareTo(PREMIUM_STANDARD) >= 0;
    }
}
