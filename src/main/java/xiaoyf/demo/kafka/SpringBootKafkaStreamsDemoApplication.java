package xiaoyf.demo.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class SpringBootKafkaStreamsDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaStreamsDemoApplication.class, args);
	}

}
