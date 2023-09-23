package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
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
import static xiaoyf.demo.kafka.helper.Const.TIMESTAMP_INPUT_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.TIMESTAMP_OUTPUT_TOPIC;

/**
 * TimestampProcessor demonstrates that output record's timestamp is derived/copied from input topic (for this example
 * simple stream)
 */
//@Component
@RequiredArgsConstructor
@Slf4j
public class TimestampProcessor {
    
    private Serde<String> stringSerde;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("TimestampProcessor use builder:" + builder);

        KStream<String, String> stream1 = builder.stream(TIMESTAMP_INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

//        stream1.map(KeyValue::new)
//                .join()
//                .to(TIMESTAMP_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
    }
}