package xiaoyf.demo.kafka.mapper;

import demo.model.CustomerOrderKey;
import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class PremiumOrderKeyMapper implements KeyValueMapper<CustomerOrderKey, PremiumOrder, PremiumOrderKey> {

    @Override
    public PremiumOrderKey apply(CustomerOrderKey key, PremiumOrder value) {
        return PremiumOrderKey.newBuilder()
                .setOrderNumber(value.getOrderNumber())
                .setCustomerNumber(value.getCustomerNumber())
                .build();
    }
}
