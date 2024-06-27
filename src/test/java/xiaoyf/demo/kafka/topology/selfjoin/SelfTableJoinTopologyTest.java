package xiaoyf.demo.kafka.topology.selfjoin;

import demo.model.PreferenceChange;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.config.SharedTopologyConfiguration;
import xiaoyf.demo.kafka.helper.testhelper.TopologyTestHelper;
import xiaoyf.demo.kafka.topology.selfjoin.streamtable.SelfTableJoinTopology;

import java.util.List;

import static demo.model.CommunicationChannel.EMAIL;
import static demo.model.CommunicationChannel.SNAIL;
import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_CHANGE_TOPIC;
import static xiaoyf.demo.kafka.helper.Const.PREFERENCE_TOPIC;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceValue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        SharedTopologyConfiguration.class,
        SelfTableJoinTopologyTest.TestConfig.class,
        SelfTableJoinTopology.class,
        PreferenceSelfJoiner.class,
    }
)
@TestPropertySource(
        properties = {"demo-streams.stream-table-self-join-app-enabled=true"}
)
@Slf4j
public class SelfTableJoinTopologyTest {

    @Autowired
    @Qualifier("streamTableSelfJoinStreamBuilder")
    private StreamsBuilder streamTableSelfJoinStreamBuilder;

    private TestInputTopic<PreferenceKey, PreferenceValue> preferenceTopic;
    private TestOutputTopic<PreferenceKey, PreferenceChange> preferenceChangeTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(streamTableSelfJoinStreamBuilder);

        log.info(helper.getTopology().describe().toString());

        preferenceTopic = helper.inputTopic(PREFERENCE_TOPIC);
        preferenceChangeTopic = helper.outputTopic(PREFERENCE_CHANGE_TOPIC);
    }

    @AfterEach
    void cleanup() {
        if (helper != null) {
            helper.close();
        };
    }

    @Test
    void shouldRaiseGreenChangeWithTableJoin() {
        PreferenceValue pref = testPreferenceValue();
        pref.setPreferredComms(SNAIL);
        preferenceTopic.pipeInput(
                testPreferenceKey(),
                pref
        );

        pref.setPreferredComms(EMAIL);
        preferenceTopic.pipeInput(
                testPreferenceKey(),
                pref
        );

        final List<KeyValue<PreferenceKey, PreferenceChange>> prefChange =
                preferenceChangeTopic.readKeyValuesToList();
        assertThat(prefChange).hasSize(2);
        assertThat(prefChange).noneSatisfy(
                kv -> assertThat(kv.value.getGreenChange()).isTrue()
        );
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("streamTableSelfJoinStreamBuilder")
        StreamsBuilder streamTableSelfJoinStreamBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setStreamTableSelfJoinAppId("self-table-join-topology-test");
            properties.setPreferenceTopic(PREFERENCE_TOPIC);
            properties.setPreferenceChangeTopic(PREFERENCE_CHANGE_TOPIC);

            return properties;
        }

        @Bean
        KafkaProperties kafkaProperties() {
            KafkaProperties properties = new KafkaProperties();
            properties.getProperties().put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy");
            return properties;
        }
    }

}
