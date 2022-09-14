package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.joiner.ClickLocationValueJoiner;

import static xiaoyf.demo.kafka.helper.Const.CLICK_PLUS_LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CLICK_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Serdes.stringSerde;

/**
 * StreamGlobalKTableJoinProcessor demonstrates a stream-globalktable join.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class StreamGlobalKTableJoinProcessor {

    private final ClickLocationValueJoiner valueJoiner;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("StreamGlobalKTableJoinProcessor use builder:" + builder);

        KStream<String, String> transactions = builder.stream(CLICK_TOPIC, Consumed.with(stringSerde(), stringSerde()));
        GlobalKTable<String, String> location = builder.globalTable(LOCATION_TOPIC, Consumed.with(stringSerde(), stringSerde()));

        transactions
                .join(location, (clickKey, clickValue) -> clickKey, valueJoiner)
                .to(CLICK_PLUS_LOCATION_TOPIC, Produced.with(stringSerde(), stringSerde()));
    }
}

/* NOTE
   1. GlobalKTable is NOT time-synchronised: When there are existing records on 'location' topic, they are read into
      GlobalKTable as first stage of stream processing before joining occurs. As a result, the following two scenarios
      yield different/inconsistent results
      - t0: start streaming application,
        t1: insert 'click'    1:C1
        t2: insert 'location' 1:L1
        -> EMPTY join output
      - t0: insert 'click'    1:C1,
        t1: insert 'location' 1:L1
        t2: start streaming application
        -> C1:L1 joined
 */