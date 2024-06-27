package xiaoyf.demo.kafka.topology.dualjoin.tabletablejoin;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TableJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="table-table-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class TableTableJoinTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;
    private final PreferenceContactJoiner joiner;

    @Autowired
    void process(@Qualifier("ttJoinStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        KTable<CustomerKey, ContactValue> contactTable = builder.<CustomerKey, ContactValue>stream(properties.getContactTopic())
                .toTable(
                        Named.as("tt-contact-table"),
                        Materialized.as("tt-contact-table")
                );

        builder.<PreferenceKey, PreferenceValue>stream(properties.getPreferenceTopic())
                .toTable(
                        Named.as("tt-pref-table"),
                        Materialized.as("tt-perf-table")
                )
                .join(
                        contactTable,
                        v -> CustomerKey.newBuilder().setCustomerNumber(v.getCustomerNumber()).build(),
                        joiner,
                        TableJoined.as("tt-pref-joins-contact")
                )
                .toStream()
                .to(properties.getEnrichedPreferenceTopic());

    }
}