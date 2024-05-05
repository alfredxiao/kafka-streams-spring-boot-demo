package xiaoyf.demo.kafka.helper.data;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;

import java.util.List;

public class TestData {
    public static final Long ORDER_NUMBER = 100L;
    public static final Long CUSTOMER_NUMBER = 2000L;
    public static final String CUSTOMER_NAME = "Alfred Xiao";
    public static final String CUSTOMER_EMAIL = "ax@email.com";

    public static OrderKey testOrderKey() {
        return testOrderKey(ORDER_NUMBER);
    }

    public static OrderKey testOrderKey(final Long orderNumber) {
        return OrderKey.newBuilder()
                .setOrderNumber(orderNumber)
                .build();
    }

    public static OrderValue testOrderValue() {
        return testOrderValue(ORDER_NUMBER);
    }

    public static OrderValue testOrderValue(final Long orderNumber) {
        return OrderValue.newBuilder()
                .setOrderNumber(orderNumber)
                .setQuantity(11L)
                .setCampaign("camp1")
                .setCustomerNumber(CUSTOMER_NUMBER)
                .setProductName("iPhone v2")
                .build();
    }

    public static CustomerKey testCustomerKey() {
        return CustomerKey.newBuilder()
                .setCustomerNumber(CUSTOMER_NUMBER)
                .build();
    }

    public static CustomerValue testCustomerValue() {
        return CustomerValue.newBuilder()
                .setCustomerNumber(CUSTOMER_NUMBER)
                .setName(CUSTOMER_NAME)
                .setEmail(CUSTOMER_EMAIL)
                .setActiveCampaigns(List.of("camp1"))
                .build();
    }
}
