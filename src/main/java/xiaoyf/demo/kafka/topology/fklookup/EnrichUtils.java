package xiaoyf.demo.kafka.topology.fklookup;

import demo.model.CustomerValue;
import demo.model.OrderEnriched;
import demo.model.OrderValue;
import lombok.experimental.UtilityClass;

@UtilityClass
public class EnrichUtils {

    public static OrderEnriched enrich(final OrderValue order, final CustomerValue customer) {
        return OrderEnriched.newBuilder()
                .setCustomer(customer)
                .setCustomerNumber(order.getCustomerNumber())
                .setCampaign(order.getCampaign())
                .setOrderNumber(order.getOrderNumber())
                .setProductName(order.getProductName())
                .setQuantity(order.getQuantity())
                .build();
    }
}
