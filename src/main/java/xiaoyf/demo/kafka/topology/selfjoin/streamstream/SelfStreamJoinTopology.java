package xiaoyf.demo.kafka.topology.selfjoin.streamstream;


import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.topology.selfjoin.PreferenceSelfJoiner;

import java.time.Duration;

import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_CHANGE_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_TOPIC;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="stream-stream-self-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class SelfStreamJoinTopology {

    private final PreferenceSelfJoiner joiner;

    @Autowired
    public void process(@Qualifier("streamStreamSelfJoinStreamBuilder") StreamsBuilder builder) {
        log.info("SelfJoinTopology use builder:" + builder);

        KStream<PreferenceKey, PreferenceValue> stream = builder.stream(PREFERENCE_TOPIC);

        stream.join(
                    stream,
                    joiner,
                    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofHours(24), Duration.ofSeconds(2)))
                .to(PREFERENCE_CHANGE_TOPIC);

    }
}