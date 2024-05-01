package xiaoyf.demo.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.time.Duration;

//@Component
@Slf4j
public class JobProcessor {
    @Autowired
    public void process(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {

        builder.<String, String>stream("t1")
                .process(() -> new ScheduledProcessor(Duration.ofSeconds(60)), Named.as("t1"))
                .to("t2");
    }

}
