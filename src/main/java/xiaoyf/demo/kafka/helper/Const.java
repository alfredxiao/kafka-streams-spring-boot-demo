package xiaoyf.demo.kafka.helper;

public interface Const {
    String BOOTSTRAP_SERVER = "localhost:9092";

    // topics for table-table-fk-join
    String CUSTOMER_ORDER_TOPIC = "customer-order";
    String CUSTOMER_DETAIL_TOPIC = "customer-detail";
    String PREMIUM_ORDER_TOPIC = "premium-order";

    // topics for stream-table-join
    String CLICK_TOPIC = "click";
    String LOCATION_TOPIC = "location";
    String CLICK_PLUS_LOCATION_TOPIC = "click-plus-location";

    String MCC_TRANSACTION_TOPIC = "mcc-transaction";
    String MCC_CATEGORY_TOPIC = "mcc-category";
    String MCC_CATEGORISED_TOPIC = "mcc-transaction-categorised";

    // topics for secondary topology configuration
    String LONG_NUMBER_TOPIC = "long-number";
    String LONG_NUMBER_DOUBLED_TOPIC = "long-number-doubled";

    // topics for stream merging
    String STREAM1_TOPIC = "stream1";
    String STREAM2_TOPIC = "stream2";
    String STREAM_MERGED_TOPIC = "stream-merged";

    // self join
    String SELF_JOIN_INPUT_TOPIC = "self-join-input";
    String SELF_JOIN_OUTPUT_TOPIC = "self-join-output";

    String TIMESTAMP_INPUT_TOPIC = "timestamp-input";
    String TIMESTAMP_LOG_APPEND_TIME_TOPIC = "timestamp-log-append-time";
    String TIMESTAMP_OUTPUT_TOPIC = "timestamp-output";

    String PRIMARY_APPLICATION_ID = "kafka-demo";
    String SECONDARY_APPLICATION_ID = "secondary-kafka-demo";
}
