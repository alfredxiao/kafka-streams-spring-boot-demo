package xiaoyf.demo.kafka.helper.serde;

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

    public MockAvroSerializer(SchemaRegistryClient __) {
        super(SharedMockSchemaRegistryClient.getInstance());
    }

    public MockAvroSerializer(SchemaRegistryClient __, Map<String, ?> ___) {
        super(SharedMockSchemaRegistryClient.getInstance());
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        log.info("!!! {} .serialize() topic:{}, class:{}, record:{}",
                this, topic, record.getClass().getName(), record);
        return super.serialize(topic, record);
    }
}
