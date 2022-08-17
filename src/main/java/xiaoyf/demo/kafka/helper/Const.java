package xiaoyf.demo.kafka.helper;

public interface Const {
    // topics for table-table-fk-join
    String CUSTOMER_ORDER_TOPIC = "customer-order";
    String CUSTOMER_DETAIL_TOPIC = "customer-detail";
    String PREMIUM_ORDER_TOPIC = "premium-order";

    // topics for stream-table-join
    String CLICK_TOPIC = "click";
    String LOCATION_TOPIC = "location";
    String CLICK_PLUS_LOCATION_TOPIC = "click-plus-location";

    // topics for secondary topology configuration
    String LONG_NUMBER_TOPIC = "long-number";
    String LONG_NUMBER_DOUBLED_TOPIC = "long-number-doubled";

    // topics for stream merging
    String STREAM1_TOPIC = "stream1";
    String STREAM2_TOPIC = "stream2";
    String STREAM_MERGED_TOPIC = "stream-merged";

    String PRIMARY_APPLICATION_ID = "kafka-demo";
    String SECONDARY_APPLICATION_ID = "secondary-kafka-demo";
}
