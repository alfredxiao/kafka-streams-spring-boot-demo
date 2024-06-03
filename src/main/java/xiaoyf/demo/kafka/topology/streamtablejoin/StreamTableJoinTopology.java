package xiaoyf.demo.kafka.topology.streamtablejoin;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.CustomerValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.helper.PropertiesLogHelper;

@Component
@RequiredArgsConstructor
@Slf4j
public class StreamTableJoinTopology {
    private final PropertiesLogHelper logHelper;
    private final DemoProperties properties;

    private final CustomerContactJoiner customerContactJoiner;

    @Autowired
    void process(@Qualifier("streamTableJoinStreamsBuilder") StreamsBuilder builder) {
        logHelper.logProperties(log);

        final KTable<CustomerKey, ContactValue> contactTable =
                builder.<CustomerKey, ContactValue>stream(properties.getContactTopic())
                        .toTable(
                                Named.as("contact-table"),
                                Materialized.as("contact-table")
                        );

        builder.<CustomerKey, CustomerValue>stream(properties.getCustomerTopic())
                .leftJoin(
                        contactTable,
                        customerContactJoiner,
                        Joined.as("customer-joins-contact")
                )
                .to(properties.getEnrichedCustomerTopic());
    }
}