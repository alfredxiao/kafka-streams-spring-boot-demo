package xiaoyf.demo.kafka.filter;

import demo.model.OrderKey;
import demo.model.OrderValue;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class BigPurchaseFilter implements Predicate<OrderKey, OrderValue> {
    private static final long BIG_PURCHASE_THRESHOLD = 1000L;

    @Override
    public boolean test(OrderKey key, OrderValue value) {
        return Objects.nonNull(value.getCampaign())
                && value.getQuantity() > BIG_PURCHASE_THRESHOLD;
    }
}
