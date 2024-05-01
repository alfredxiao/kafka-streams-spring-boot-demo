package xiaoyf.demo.kafka.actuator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsHealthIndicator implements HealthIndicator {
    private final List<StreamsBuilderFactoryBean> streamsBuilders;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public Health health() {
        for (StreamsBuilderFactoryBean streamsBuilderFactoryBean : streamsBuilders) {
            final Properties conf = streamsBuilderFactoryBean.getStreamsConfiguration();
            final KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
            final Topology topology = streamsBuilderFactoryBean.getTopology();

            if (conf == null) {
                log.warn("No Kafka Streams Configuration found from streams builder: {}", streamsBuilderFactoryBean);
                continue;
            }

            if (topology == null) {
                log.warn("No Kafka Streams Topology found from streams builder via config: {}", conf);
                continue;
            }

            if (streams == null) {
                log.warn("No KafkaStreams instance found from topology: {}", topology.describe());
                continue;
            }

            boolean running = streams.state().isRunningOrRebalancing();
            if (!running) {
                log.warn("stream {}: status is NOT running or rebalancing", conf.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
                return Health.down().build();
            }
        }

        logger.debug("stream status is UP running or rebalancing");
        return Health.up().build();
    }
}
