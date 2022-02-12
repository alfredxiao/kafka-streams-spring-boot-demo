package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class MockAvroSerializer extends KafkaAvroSerializer {

    public MockAvroSerializer() {
        super.autoRegisterSchema = true;
        super.schemaRegistry = SharedMockSchemaRegistryClient.getInstance();
    }

    public MockAvroSerializer(SchemaRegistryClient client) {
        super(SharedMockSchemaRegistryClient.getInstance());
    }

    public MockAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(SharedMockSchemaRegistryClient.getInstance(), props);
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        log.info("!!! @{} .serialize() topic: {}, class: {}, record:{}",
                this.hashCode(), topic, record.getClass().getName(), record);

        if ("kafka-demo-ORDER-JOINS-CUST-DETAILS-changelog".equals(topic)) {
            int c = 3;
        }

        return super.serialize(topic, record);
    }
}
