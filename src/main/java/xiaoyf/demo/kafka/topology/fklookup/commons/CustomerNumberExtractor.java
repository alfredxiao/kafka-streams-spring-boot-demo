package xiaoyf.demo.kafka.topology.fklookup.commons;

import demo.model.CustomerKey;
import demo.model.OrderKey;
import demo.model.OrderValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.springframework.stereotype.Component;

@Component
public class CustomerNumberExtractor implements KeyValueMapper<OrderKey, OrderValue, CustomerKey> {
    @Override
    public CustomerKey apply(OrderKey key, OrderValue value) {
        if (value.getCustomerNumber() == null) {
            return null;
        }

        return CustomerKey.newBuilder()
                .setCustomerNumber(value.getCustomerNumber())
                .build();
    }
}
