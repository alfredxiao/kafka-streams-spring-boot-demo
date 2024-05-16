package xiaoyf.demo.kafka.helper;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import org.apache.kafka.streams.test.TestRecord;

import java.util.List;

public class Fixtures {

    public static TestRecord<OrderKey, OrderValue> orderValue(
            long orderNum,
            long custNum,
            String prodName,
            long quantity,
            String campaign
    ) {
        var key = OrderKey.newBuilder()
                .setOrderNumber(orderNum)
                .build();

        var value = OrderValue.newBuilder()
                .setOrderNumber(orderNum)
                .setCustomerNumber(custNum)
                .setProductName(prodName)
                .setQuantity(quantity)
                .setCampaign(campaign)
                .build();

        return new TestRecord<>(key, value);
    }

    public static TestRecord<CustomerKey, CustomerValue> customerValue(
            long custNum,
            String name,
            String email,
            String ...campaigns
    ) {
        var key = CustomerKey.newBuilder()
                .setCustomerNumber(custNum)
                .build();

        var value = CustomerValue.newBuilder()
                .setCustomerNumber(custNum)
                .setName(name)
                .setActiveCampaigns(List.of(campaigns))
                .build();

        return new TestRecord<>(key, value);
    }

}
