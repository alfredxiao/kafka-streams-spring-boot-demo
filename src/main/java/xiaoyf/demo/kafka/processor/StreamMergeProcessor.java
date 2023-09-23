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
 * StreamMergeProcessor demonstrates a stream merging can be done via different approaches. Although their functionalities
 * are the same, their topologies are different.
 */
//@Component
@RequiredArgsConstructor
@Slf4j
public class StreamMergeProcessor {
    
    private Serde<String> stringSerde;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("StreamMergeProcessor use builder:" + builder);

        // no matter which merge approach to use, outcome observed from output topic is the same
        // but topologies are different among the approaches
        merge3(builder);
    }

    private void merge1(final StreamsBuilder builder) {

        KStream<String, String> stream1 = builder.stream(STREAM1_TOPIC, Consumed.with(stringSerde, stringSerde));
        KStream<String, String> stream2 = builder.stream(STREAM2_TOPIC, Consumed.with(stringSerde, stringSerde));

        stream1
                .mapValues(v -> v + ".added")
                .to(STREAM_MERGED_TOPIC, Produced.with(stringSerde, stringSerde));
        stream2
                .mapValues(v -> v + ".added")
                .to(STREAM_MERGED_TOPIC, Produced.with(stringSerde, stringSerde));

    }

    private void merge2(final StreamsBuilder builder) {

        KStream<String, String> stream1 = builder.<String, String>stream(STREAM1_TOPIC, Consumed.with(stringSerde, stringSerde)).mapValues(v -> v + ".added");
        KStream<String, String> stream2 = builder.<String, String>stream(STREAM2_TOPIC, Consumed.with(stringSerde, stringSerde)).mapValues(v -> v + ".added");

        stream1
                .merge(stream2)
                .to(STREAM_MERGED_TOPIC, Produced.with(stringSerde, stringSerde));
    }

    private void merge3(final StreamsBuilder builder) {

        KStream<String, String> stream1 = builder.stream(STREAM1_TOPIC, Consumed.with(stringSerde, stringSerde));
        KStream<String, String> stream2 = builder.stream(STREAM2_TOPIC, Consumed.with(stringSerde, stringSerde));

        stream1
                .merge(stream2)
                .mapValues(v -> v + ".added")
                .to(STREAM_MERGED_TOPIC, Produced.with(stringSerde, stringSerde));

    }
}