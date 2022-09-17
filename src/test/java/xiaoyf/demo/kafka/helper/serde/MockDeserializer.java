package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import xiaoyf.demo.kafka.helper.Const;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;

@Slf4j
public class MockDeserializer extends KafkaAvroDeserializer {

    private final SingletonNonAvroRegistry nonAvroRegistry;

    public MockDeserializer() {
        super.schemaRegistry = SingletonMockSchemaRegistryClient.getInstance();
        this.nonAvroRegistry = SingletonNonAvroRegistry.getInstance();
        super.useSpecificAvroReader = true;
    }

    // below two constructors are added just in case they are used (like parent class)
    public MockDeserializer(SchemaRegistryClient client) {
        this();
    }

    public MockDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        this();
        this.configure(deserializerConfig(props));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        super.useSpecificAvroReader = true;
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        Deserializer<?> deserializer = this.nonAvroRegistry.getNonAvroDeserializer(topic);
        if (deserializer != null) {
            log.info("!!! @{} .deserialize() topic: {}, isKey: {}, bytes: {}, nonAvro: true", this.hashCode(), topic, isDeserializingKey(), bytes.length);
            Object result = deserializer.deserialize(topic, bytes);
            log.info("!!! @{} .deserialize() return class: {}, content: {}", this.hashCode(), result.getClass().getName(), result);

            return result;
        }

        int id = readId(bytes);

        log.info("!!! @{} .deserialize() topic: {}, isKey: {}, bytes: {}, id: {}", this.hashCode(), topic, isDeserializingKey(), bytes.length, id);
        Object result = super.deserialize(topic, bytes);
        log.info("!!! @{} .deserialize() result class: {}, content: {}", this.hashCode(), result.getClass().getName(), result);

        return result;
    }

    private Object isDeserializingKey() {
        try {
            Field field = KafkaAvroDeserializer.class.getDeclaredField("isKey");
            field.setAccessible(true);
            return field.get(this);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot access private field isKey", e);
        }
    }

    private int readId(byte[] bytes) {
        if (bytes.length < 5 || bytes[0] != 0) {
            // this is not Avro bytes
            return -2;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes, 1, 4);
        return buffer.getInt();
    }
}
