package xiaoyf.demo.kafka.helper;

import demo.model.CustomerValue;
import demo.model.CustomerKey;
import demo.model.OrderValue;
import demo.model.OrderKey;
import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.test.TestRecord;

import java.math.BigDecimal;
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
                .setEmail(email)
                .setActiveCampaigns(List.of(campaigns))
                .build();

        return new TestRecord<>(key, value);
    }

    public static KeyValue<PremiumOrderKey, PremiumOrder> premiumOrder(
            long orderNum,
            long custNum,
            String prodName,
            BigDecimal amount,
            String campaign,
            String name,
            String email
    ) {
        var key = PremiumOrderKey.newBuilder()
                .setOrderNumber(orderNum)
                .setCustomerNumber(custNum)
                .build();

        var value = PremiumOrder.newBuilder()
                .setOrderNumber(orderNum)
                .setCustomerNumber(custNum)
                .setProductName(prodName)
                .setAmount(amount)
                .setCampaign(campaign)
                .setName(name)
                .setEmail(email)
                .build();

        return new KeyValue<>(key, value);
    }
}
