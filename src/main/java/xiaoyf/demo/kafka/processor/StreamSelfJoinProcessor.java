package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static xiaoyf.demo.kafka.helper.Const.SELF_JOIN_INPUT_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.SELF_JOIN_OUTPUT_TOPIC;

/**
 * StreamSelfJoinProcessor
 */
//@Component
@RequiredArgsConstructor
@Slf4j
public class StreamSelfJoinProcessor {
    
    private Serde<String> stringSerde;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("StreamSelfJoinProcessor use builder:" + builder);

        KStream<String, String> stream1 = builder.stream(SELF_JOIN_INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

        stream1.join(
                    stream1,
                    (v1, v2) -> {
                        log.info("SEEN V1:" + v1);
                        log.info("SEEN V2:" + v2);
                        return v1 + v2;
                    },
                    JoinWindows.of(Duration.ofSeconds(5)))
                .to(SELF_JOIN_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
    }
}