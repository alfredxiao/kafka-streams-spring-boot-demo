package xiaoyf.demo.kafka.topology.fklookup.byglobalktable;

import demo.model.CustomerValue;
import demo.model.OrderEnriched;
import demo.model.OrderValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import static xiaoyf.demo.kafka.topology.fklookup.EnrichUtils.enrich;

@Component
public class OrderCustomerJoiner implements ValueJoiner<OrderValue, CustomerValue, OrderEnriched>{
    @Override
    public OrderEnriched apply(OrderValue order, CustomerValue customer) {
        return enrich(order, customer);
    }
}
