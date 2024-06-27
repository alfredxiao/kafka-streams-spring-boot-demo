package xiaoyf.demo.kafka.topology.selfjoin.streamtable;


import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.topology.selfjoin.PreferenceSelfJoiner;

import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_CHANGE_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_TOPIC;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="stream-table-self-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class SelfTableJoinTopology {

    private final PreferenceSelfJoiner joiner;
    private final DemoProperties properties;

    @Autowired
    public void process(@Qualifier("streamTableSelfJoinStreamBuilder") StreamsBuilder builder) {
        log.info("SelfTableJoinTopology use builder:" + builder);

        KStream<PreferenceKey, PreferenceValue> stream = builder.stream(properties.getPreferenceTopic());

        KTable<PreferenceKey, PreferenceValue> prefTable = stream.toTable(Materialized.as("preference-table"));

        stream.join(
                    prefTable,
                    joiner)
                .peek((k,v) -> log.info("k={},v={}",k,v))
                .to(properties.getPreferenceChangeTopic());

    }
}