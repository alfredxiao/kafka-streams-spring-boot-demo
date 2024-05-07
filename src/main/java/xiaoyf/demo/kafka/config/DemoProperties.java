package xiaoyf.demo.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "demo-streams")
@Data
public class DemoProperties {

    private String mappingAppId;

    private String dedupeAppId;
    private String fkLookupByGlobalStoreAppId;
    private String fkLookupByGlobalKTableAppId;
    private String fkLookupByRegularStoreAppId;
    private String fkLookupByJoiningAppId;
    private String scheduledJobAppId;

    private Duration scheduleInterval;

    private String orderTopic;
    private String orderDedupedTopic;
    private String customerTopic;
    private String orderEnrichedByGlobalStoreTopic;
    private String orderEnrichedByGlobalKTableTopic;
    private String orderEnrichedByRegularStoreTopic;
    private String orderEnrichedByJoiningTopic;
    private String customerOrderBatchTopic;
}
