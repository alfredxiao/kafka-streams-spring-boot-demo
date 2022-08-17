package xiaoyf.demo.kafka.filter;

import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Objects;

@Component
public class BigPurchaseFilter implements Predicate<CustomerOrderKey, CustomerOrder> {
    private static final BigDecimal BIG_PURCHASE_THRESHOLD = new BigDecimal("1000.00");

    @Override
    public boolean test(CustomerOrderKey key, CustomerOrder value) {
        return Objects.nonNull(value.getCampaign())
                && value.getAmount().compareTo(BIG_PURCHASE_THRESHOLD) >= 0;
    }
}
