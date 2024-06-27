package xiaoyf.demo.kafka.helper;

public interface Const {
    String BOOTSTRAP_SERVER = "localhost:9092";

    // topics for table-table-fk-join
    String CUSTOMER_ORDER_TOPIC = "customer-order";
    String CUSTOMER_DETAIL_TOPIC = "customer-detail";
    String PREMIUM_ORDER_TOPIC = "premium-order";

    // topics for stream merging
    String STREAM1_TOPIC = "stream1";
    String STREAM2_TOPIC = "stream2";
    String STREAM_MERGED_TOPIC = "stream-merged";

    // self join
    String SELF_JOIN_INPUT_TOPIC = "self-join-input";
    String SELF_JOIN_OUTPUT_TOPIC = "self-join-output";

    String TIMESTAMP_INPUT_TOPIC = "timestamp-input";
    String TIMESTAMP_LOG_APPEND_TIME_TOPIC = "timestamp-log-append-time";

    String PRIMARY_APPLICATION_ID = "kafka-demo";

    String PREFERENCE_TOPIC = "preference";
    String PREFERENCE_CHANGE_TOPIC = "preference-change";
    String CONTACT_TOPIC = "contact";
    String CUSTOMER_TOPIC = "customer";
    String ORDER_TOPIC = "order";
}
