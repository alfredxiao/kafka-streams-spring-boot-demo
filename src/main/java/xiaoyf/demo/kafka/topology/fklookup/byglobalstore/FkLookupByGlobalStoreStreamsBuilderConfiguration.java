package xiaoyf.demo.kafka.topology.fklookup.byglobalstore;

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
public class FkLookupByGlobalStoreStreamsBuilderConfiguration {

    private final DemoProperties properties;
    @Bean("fkLookupByGlobalStoreStreamsBuilder")
    public StreamsBuilderFactoryBean fkLookupByGlobalStoreStreamsBuilder(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties(null);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getFkLookupByGlobalStoreAppId());

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }
}
