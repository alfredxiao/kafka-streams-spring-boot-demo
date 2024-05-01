package xiaoyf.demo.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import xiaoyf.demo.kafka.config.DemoProperties;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(DemoProperties.class)
public class SpringBootKafkaStreamsDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaStreamsDemoApplication.class, args);
	}

}
