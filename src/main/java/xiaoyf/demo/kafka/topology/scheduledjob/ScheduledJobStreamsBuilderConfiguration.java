package xiaoyf.demo.kafka.topology.scheduledjob;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import xiaoyf.demo.kafka.config.DemoProperties;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class ScheduledJobStreamsBuilderConfiguration {

    private final DemoProperties properties;

    @Bean("scheduledStreamsBuilder")
    public StreamsBuilderFactoryBean scheduledStreamsBuilder(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties(null);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getScheduledJobAppId());

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }
}
