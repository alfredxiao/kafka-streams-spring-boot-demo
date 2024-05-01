package xiaoyf.demo.kafka.mapper;

import demo.model.OrderKey;
import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class PremiumOrderKeyMapper implements KeyValueMapper<OrderKey, PremiumOrder, PremiumOrderKey> {

    @Override
    public PremiumOrderKey apply(OrderKey key, PremiumOrder value) {
        return PremiumOrderKey.newBuilder()
                .setOrderNumber(value.getOrderNumber())
                .setCustomerNumber(value.getCustomerNumber())
                .build();
    }
}
