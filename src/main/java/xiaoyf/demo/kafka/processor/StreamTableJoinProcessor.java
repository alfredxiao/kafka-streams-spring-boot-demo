package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.joiner.ClickLocationValueJoiner;

import static xiaoyf.demo.kafka.helper.Const.CLICK_PLUS_LOCATION_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.CLICK_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LOCATION_TOPIC;

/**
 * StreamTableJoinProcessor demonstrates a stream-table join
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class StreamTableJoinProcessor {

    private final ClickLocationValueJoiner valueJoiner;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("StreamTableJoinProcessor use builder:" + builder);

        // Note that 'location' is used as source topic for a table as first step of a topology, if optimization is
        // enabled, Kafka Streams won't create changelog topic for it and expect this topic itself is a compacted topic.
        KTable<String, String> location = builder.table(LOCATION_TOPIC);
        KStream<String, String> transactions = builder.stream(CLICK_TOPIC);

        transactions
                .join(location, valueJoiner)
                .to(CLICK_PLUS_LOCATION_TOPIC);
    }

}