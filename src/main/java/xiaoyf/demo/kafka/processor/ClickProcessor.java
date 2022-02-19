package xiaoyf.demo.kafka.processor;


import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import xiaoyf.demo.kafka.joiner.ClickLocationValueJoiner;

/**
 * ClickProcessor demonstrates a stream-table join
 */
@Component
@RequiredArgsConstructor
public class ClickProcessor {

    private final ClickLocationValueJoiner valueJoiner;

    @Autowired
    public void process(StreamsBuilder builder) {

        System.out.println("builder=" + builder);

        KTable<String, String> location = builder.table("location");

        KStream<String, String> transactions = builder.stream("click");

        transactions
                .join(location, valueJoiner)
                .to("click-location");
    }

}