package xiaoyf.demo.kafka.config;


import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import xiaoyf.demo.kafka.commons.config.GenericSerdeFactory;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class SharedTopologyConfiguration {

    @Bean
    public GenericSerdeFactory genericSerdeFactory(final KafkaProperties kafkaProperties) {
        return new GenericSerdeFactory(kafkaProperties.buildStreamsProperties(null));
    }

    @Bean
    public Serde<OrderKey> orderKeySerde(final GenericSerdeFactory genericSerdeFactory) {
        return genericSerdeFactory.keySerde();
    }

    @Bean
    public Serde<OrderValue> orderValueSerde(final GenericSerdeFactory genericSerdeFactory) {
        return genericSerdeFactory.valueSerde();
    }

    @Bean
    public Serde<CustomerKey> customerKeySerde(final GenericSerdeFactory genericSerdeFactory) {
        return genericSerdeFactory.keySerde();
    }

    @Bean
    public Serde<CustomerValue> customerValueSerde(final GenericSerdeFactory genericSerdeFactory) {
        return genericSerdeFactory.valueSerde();
    }
}
