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

import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.LONG_NUMBER_DOUBLED_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.SECONDARY_APPLICATION_ID;

/**
 * AnotherTopologyConfiguration demonstrates you can run more than one topology in the same SpringBoot
 * application. This class also demonstrates another way to create a topology - by returning a bean of
 * type KStream.
 */
@Configuration
@Slf4j
public class SecondaryTopologyConfiguration {
    @Bean("secondaryKafkaStreamBuilder")
    public StreamsBuilderFactoryBean secondaryKafkaStreamBuilderFactoryBean(KafkaProperties props) {

        Map<String, Object> config = props.buildStreamsProperties();
        log.info("secondaryKafkaStreamBuilder built on top of kafka props: {}", config);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, SECONDARY_APPLICATION_ID);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

        return new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(config));
    }

    @Bean
    public KStream<Long, Long> secondaryProcessing(@Qualifier("secondaryKafkaStreamBuilder") StreamsBuilder builder) {

        final KStream<Long, Long> toDouble = builder.stream(LONG_NUMBER_TOPIC);

        toDouble
                .map((key, value) -> {
                    log.info("Processing long message: {}, {}", key, value);
                    return KeyValue.pair(key, value * 2);
                })
                .to(LONG_NUMBER_DOUBLED_TOPIC); // send downstream to another topic

        return toDouble;
    }
}
