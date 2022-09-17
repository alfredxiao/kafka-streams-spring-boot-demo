package xiaoyf.demo.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import xiaoyf.demo.kafka.helper.Const;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static xiaoyf.demo.kafka.helper.Const.CLICK_PLUS_LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CLICK_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_DETAIL_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CUSTOMER_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_DOUBLED_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_CATEGORISED_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_CATEGORY_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.MCC_TRANSACTION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREMIUM_ORDER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PRIMARY_APPLICATION_ID;
import static xiaoyf.demo.kafka.helper.Const.SECONDARY_APPLICATION_ID;
import static xiaoyf.demo.kafka.helper.Const.STREAM1_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM2_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM_MERGED_TOPIC;


@Configuration
@ConditionalOnProperty(value="init-create-topics", havingValue="true")
@Slf4j
public class InitTopicsConfiguration {
    @PostConstruct
    public void createTopics() throws Exception {
        final Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Const.BOOTSTRAP_SERVER);
        final AdminClient adminClient = AdminClient.create(config);
        final int numPartitions = 1;
        final short rf = 1;

        List<String> topicNames = List.of(
                CUSTOMER_ORDER_TOPIC,
                CUSTOMER_DETAIL_TOPIC,
                PREMIUM_ORDER_TOPIC,
                CLICK_TOPIC,
                LOCATION_TOPIC,
                CLICK_PLUS_LOCATION_TOPIC,
                MCC_TRANSACTION_TOPIC,
                MCC_CATEGORY_TOPIC,
                MCC_CATEGORISED_TOPIC,
                LONG_NUMBER_TOPIC,
                LONG_NUMBER_DOUBLED_TOPIC,
                STREAM1_TOPIC,
                STREAM2_TOPIC,
                STREAM_MERGED_TOPIC,
                PRIMARY_APPLICATION_ID,
                SECONDARY_APPLICATION_ID
        );

        Set<String> existingTopics = adminClient.listTopics().names().get();

        for (String topicName : topicNames) {
            if (!existingTopics.contains(topicName)) {
                final NewTopic newTopic = new NewTopic(topicName, numPartitions, rf);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            }
        }
    }
}
