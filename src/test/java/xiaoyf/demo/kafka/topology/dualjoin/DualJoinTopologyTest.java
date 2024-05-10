package xiaoyf.demo.kafka.topology.dualjoin;

import demo.model.ContactValue;
import demo.model.CustomerKey;
import demo.model.EnrichedPreferenceValue;
import demo.model.PreferenceKey;
import demo.model.PreferenceValue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import org.springframework.test.context.junit.jupiter.SpringExtension;
import xiaoyf.demo.kafka.config.DemoProperties;
import xiaoyf.demo.kafka.config.SharedTopologyConfiguration;
import xiaoyf.demo.kafka.helper.testhelper.TopologyTestHelper;

import java.util.List;

import static demo.model.CommunicationChannel.SNAIL;
import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.testContactValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testPreferenceValue;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
    classes = {
        SharedTopologyConfiguration.class,
        DualJoinTopologyTest.TestConfig.class,
        DualJoinTopology.class,
        DualJoinTopologyConfiguration.class,
    }
)
public class DualJoinTopologyTest {
    final static String PREFERENCE_TOPIC = "preference";
    final static String CONTACT_TOPIC = "contact";
    final static String PREFERENCE_ENRICHED_TOPIC = "preference-enriched";

    @Autowired
    @Qualifier("dualJoinStreamsBuilder")
    private StreamsBuilder dualJoinStreamsBuilder;

    private TestInputTopic<PreferenceKey, PreferenceValue> preferenceTopic;
    private TestInputTopic<CustomerKey, ContactValue> contactTopic;
    private TestOutputTopic<CustomerKey, EnrichedPreferenceValue> preferenceEnrichedTopic;
    private TopologyTestHelper helper;

    @BeforeEach
    void setup() {
        helper = new TopologyTestHelper(dualJoinStreamsBuilder);

        preferenceTopic = helper.inputTopic(PREFERENCE_TOPIC);
        contactTopic = helper.inputTopic(CONTACT_TOPIC);
        preferenceEnrichedTopic = helper.outputTopic(PREFERENCE_ENRICHED_TOPIC);
    }

    @AfterEach
    void cleanup() {
        if (helper != null) {
            helper.close();
        };
    }

    @Test
    void shouldEnrichPreferenceWhenContactComesLate() {
        preferenceTopic.pipeInput(
                testPreferenceKey(),
                testPreferenceValue()
        );
        contactTopic.pipeInput(
                testCustomerKey(),
                testContactValue()
        );

        final List<KeyValue<CustomerKey, EnrichedPreferenceValue>> prefsEnriched =
                preferenceEnrichedTopic.readKeyValuesToList();
        assertThat(prefsEnriched).hasSize(1);
        assertThat(prefsEnriched.get(0).value).isNotNull();
        assertThat(prefsEnriched.get(0).value.getContact()).isEqualTo(testContactValue());
    }

    @Test
    void shouldEnrichPreferenceAgainWhenPreferenceUpdateComesLater() {
        preferenceTopic.pipeInput(
                testPreferenceKey(),
                testPreferenceValue()
        );
        contactTopic.pipeInput(
                testCustomerKey(),
                testContactValue()
        );

        PreferenceKey preferenceKey = testPreferenceKey();
        PreferenceValue preferenceValue = testPreferenceValue();

        preferenceKey.setPreferenceNumber(8001L);
        preferenceValue.setPreferredComms(SNAIL);

        preferenceTopic.pipeInput(preferenceKey, preferenceValue);

        final List<KeyValue<CustomerKey, EnrichedPreferenceValue>> prefsEnriched =
                preferenceEnrichedTopic.readKeyValuesToList();

        assertThat(prefsEnriched).hasSize(2);

        assertThat(prefsEnriched.get(1).value).isNotNull();
        assertThat(prefsEnriched.get(1).value.getPreferredComms()).isEqualTo(SNAIL);
    }

    @Test
    void shouldEnrichPreferenceAgainWhenContactUpdateComesLater() {
        contactTopic.pipeInput(
                testCustomerKey(),
                testContactValue()
        );
        preferenceTopic.pipeInput(
                testPreferenceKey(),
                testPreferenceValue()
        );

        CustomerKey customerKey = testCustomerKey();
        ContactValue contactValue = testContactValue();

        final String newAddress = "New Address";
        contactValue.setAddress(newAddress);

        contactTopic.pipeInput(customerKey, contactValue);

        final List<KeyValue<CustomerKey, EnrichedPreferenceValue>> prefsEnriched =
                preferenceEnrichedTopic.readKeyValuesToList();

        assertThat(prefsEnriched).hasSize(2);

        assertThat(prefsEnriched.get(1).value).isNotNull();
        assertThat(prefsEnriched.get(1).value.getContact()).isNotNull();
        assertThat(prefsEnriched.get(1).value.getContact().getAddress()).isEqualTo(newAddress);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean("dualJoinStreamsBuilder")
        StreamsBuilder dualJoinStreamsBuilder() {
            return new StreamsBuilder();
        }

        @Bean
        DemoProperties demoProperties() {
            DemoProperties properties = new DemoProperties();

            properties.setDualJoinAppId("dual-join-topology-test");
            properties.setPreferenceTopic(PREFERENCE_TOPIC);
            properties.setContactTopic(CONTACT_TOPIC);
            properties.setEnrichedPreferenceTopic(PREFERENCE_ENRICHED_TOPIC);

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
