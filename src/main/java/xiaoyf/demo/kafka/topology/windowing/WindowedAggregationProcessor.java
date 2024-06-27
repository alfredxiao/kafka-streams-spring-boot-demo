package xiaoyf.demo.kafka.topology.windowing;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static xiaoyf.demo.kafka.helper.Const.STREAM1_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM2_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.STREAM_MERGED_TOPIC;

/**
 * WindowedAggregationProcessor demonstrates
 */
//@Component
@RequiredArgsConstructor
@Slf4j
public class WindowedAggregationProcessor {
    
    private Serde<String> stringSerde;

    // If there is no need for running multiple topologies, no 'Qualifier' is needed because there is only one
    // StreamsBuilder instance then
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("WindowedAggregationProcessor use builder:" + builder);

        KStream<String, String> input = builder.stream(STREAM1_TOPIC, Consumed.with(stringSerde, stringSerde));

        KStream<Windowed<String>, HashMap<String, String>> aggregated = input
                .groupBy((k, v) -> v.substring(0, 2))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .aggregate(
                        () -> new HashMap<String, String>(),
                        (key, value, aggregate) -> {
                            aggregate.put(key, value);
                            return aggregate;
                        }
                )
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream();

        KTable<String, ArrayList<String>> summarised = aggregated
                .mapValues(agg -> {
                    return new HashMap<>();
                })
                .groupBy((k, v) -> k.key())
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value.toString());
                            return aggregate;
                        }
                );

        aggregated
                .selectKey((k, v) -> k.key())
                .split()
                .branch(
                        (k, v) -> true, // estimate
                        Branched.withConsumer(ks -> ks.to("output"))
                )
                .branch(
                        (k, v) -> false, // predict
                        Branched.withConsumer(ks -> ks
                                .join(summarised, (k, v1, v2) -> v2)
                                .to("output")
                        )
                );
    }


}