package xiaoyf.demo.kafka.config;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerdeConfiguration {

    @Bean
    Serde<String> stringSerde() {
        return Serdes.String();
    }
}
