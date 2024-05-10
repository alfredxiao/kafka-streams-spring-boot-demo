package xiaoyf.demo.kafka.helper.data;

import demo.model.CommunicationChannel;
import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;

import java.util.List;

public class TestData {
    public static final Long ORDER_NUMBER = 100L;
    public static final Long CUSTOMER_NUMBER = 2000L;
    public static final String CUSTOMER_NAME = "Alfred Xiao";
    private static final long PREFERENCE_NUMBER = 3000L;
    private static final String EMAIL_ADDRESS = "alfred@email.com";
    private static final String PHONE_NUMBER = "012345678";

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
        return testOrderValue(orderNumber, CUSTOMER_NUMBER);
    }

    public static OrderValue testOrderValue(final Long orderNumber, final Long customerNumber) {
        return OrderValue.newBuilder()
                .setOrderNumber(orderNumber)
                .setQuantity(11L)
                .setCampaign("camp1")
                .setCustomerNumber(customerNumber)
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
                .setActiveCampaigns(List.of("camp1"))
                .build();
    }

    public static PreferenceKey testPreferenceKey() {
        return PreferenceKey.newBuilder()
                .setPreferenceNumber(PREFERENCE_NUMBER)
                .build();
    }

    public static PreferenceValue testPreferenceValue() {
        return PreferenceValue.newBuilder()
                .setCustomerNumber(CUSTOMER_NUMBER)
                .setPreferredComms(CommunicationChannel.SMS)
                .build();
    }

    public static ContactValue testContactValue() {
        return ContactValue.newBuilder()
                .setEmail(EMAIL_ADDRESS)
                .setPhone(PHONE_NUMBER)
                .build();
    }
}
