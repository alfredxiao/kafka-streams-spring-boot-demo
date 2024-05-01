package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import static xiaoyf.demo.kafka.helper.Const.STREAM1_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM2_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM_MERGED_TOPIC;

/**
 * GroupbyThenCountProcessor demonstrates how groupBy & count works
 */
//@Component
@RequiredArgsConstructor
@Slf4j
public class GroupByThenCountProcessor {
    
    private Serde<String> stringSerde;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("GroupByThenCountProcessor use builder:" + builder);

        // Alfred:House1, Alfred:House2
        KStream<String, String> userHouseStream = builder.stream("user_house_event", Consumed.with(stringSerde, stringSerde));

        userHouseStream
                .groupByKey()
                .count()
                .toStream()
                .mapValues((k,v) -> "" + v)
                .to("user_house_count");

    }
}