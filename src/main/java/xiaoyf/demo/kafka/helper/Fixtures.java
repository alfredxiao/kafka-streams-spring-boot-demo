package xiaoyf.demo.kafka.helper;

import demo.model.CustomerDetails;
import demo.model.CustomerDetailsKey;
import demo.model.CustomerOrder;
import demo.model.CustomerOrderKey;
import demo.model.PremiumOrder;
import demo.model.PremiumOrderKey;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.test.TestRecord;

import java.math.BigDecimal;
import java.util.List;

public class Fixtures {

    public static TestRecord<CustomerOrderKey, CustomerOrder> customerOrder(
            long orderNum,
            long custNum,
            String prodName,
            BigDecimal amount,
            String campaign
    ) {
        var key = CustomerOrderKey.newBuilder()
                .setOrderNumber(orderNum)
                .build();

        var value = CustomerOrder.newBuilder()
                .setOrderNumber(orderNum)
                .setCustomerNumber(custNum)
                .setProductName(prodName)
                .setAmount(amount)
                .setCampaign(campaign)
                .build();

        return new TestRecord<>(key, value);
    }

    public static TestRecord<CustomerDetailsKey, CustomerDetails> customerDetail(
            long custNum,
            String name,
            String email,
            String ...campaigns
    ) {
        var key = CustomerDetailsKey.newBuilder()
                .setCustomerNumber(custNum)
                .build();

        var value = CustomerDetails.newBuilder()
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
