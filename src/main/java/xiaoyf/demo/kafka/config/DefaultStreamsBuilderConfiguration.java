package xiaoyf.demo.kafka.config;

import demo.model.OrderValue;
import demo.model.OrderKey;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@Configuration
@RequiredArgsConstructor
public class DefaultStreamsBuilderConfiguration {
    private final DemoProperties properties;

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
        return sfb -> sfb.setAutoStartup(false);
    }

    @Bean
    KStream<OrderKey, OrderValue> noop(@Qualifier("defaultKafkaStreamsBuilder") StreamsBuilder builder) {
        return builder.stream(properties.getOrderTopic());
    }

}
