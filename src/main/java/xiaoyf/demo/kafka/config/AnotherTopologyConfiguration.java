package xiaoyf.demo.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Map;

/**
 * AnotherTopologyConfiguration demonstrates you can run two topology in the same SpringBoot
 * application.
 */
@Configuration
@Slf4j
public class AnotherTopologyConfiguration {
    @Bean("anotherStreamBuilder")
    public StreamsBuilderFactoryBean anotherStreamBuilderFactoryBean(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties();
        log.info("anotherStreamBuilder built on top of kafka props: {}", config);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "another-kafka-demo");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }

    @Bean
    public KStream<Long, Long> startProcessing(@Qualifier("anotherStreamBuilder") StreamsBuilder builder) {

        final KStream<Long, Long> toDouble = builder.stream("another-topic");

        toDouble
                .map((key, value) -> {
                    log.info("Processing long message: {}, {}", key, value);
                    return KeyValue.pair(key, value * 2);
                })
                .to("another-topic-doubled"); // send downstream to another topic

        return toDouble;
    }
}
