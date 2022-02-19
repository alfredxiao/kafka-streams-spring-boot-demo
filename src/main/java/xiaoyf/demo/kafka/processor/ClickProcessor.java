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

/**
 * ClickProcessor demonstrates a stream-table join
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ClickProcessor {

    private final ClickLocationValueJoiner valueJoiner;

    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        log.info("ClickProcessor use builder:" + builder);

        KTable<String, String> location = builder.table("location");

        KStream<String, String> transactions = builder.stream("click");

        transactions
                .join(location, valueJoiner)
                .to("click-location");
    }

}