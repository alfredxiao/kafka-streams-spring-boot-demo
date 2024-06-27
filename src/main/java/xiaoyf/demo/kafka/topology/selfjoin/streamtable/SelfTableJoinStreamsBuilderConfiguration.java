package xiaoyf.demo.kafka.topology.selfjoin.streamtable;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import xiaoyf.demo.kafka.config.DemoProperties;

import java.util.Map;

@Configuration
@ConditionalOnProperty(
        prefix="demo-streams",
        name="stream-table-self-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
public class SelfTableJoinStreamsBuilderConfiguration {

    private final DemoProperties properties;
    @Bean("streamTableSelfJoinStreamBuilder")
    public StreamsBuilderFactoryBean streamTableSelfJoinStreamBuilder(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties(null);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getStreamTableSelfJoinAppId());

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }
}
