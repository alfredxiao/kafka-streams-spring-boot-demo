package xiaoyf.demo.kafka.topology.idshortener.registry;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
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
public class ShortIdRegistryStreamsBuilderConfiguration {

    private final DemoProperties properties;
    @Bean("shortIdRegistryStreamsBuilder")
    public StreamsBuilderFactoryBean shortIdRegistryStreamsBuilder(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties(null);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getShortIdRegistryAppId());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }
}
