package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

@Slf4j
public class MockAvroDeserializer extends KafkaAvroDeserializer {

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        int id = readId(bytes);
        log.info("!!! @{} .deserialize() topic: {}, isKey: {}, bytes: {}, id: {}", this.hashCode(), topic, isDeserializingKey(), bytes.length, id);

        this.schemaRegistry = SharedMockSchemaRegistryClient.getInstance();

        super.useSpecificAvroReader = true;
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
        if (bytes.length < 5) {
            // this is not Avro bytes
            return -2;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes, 1, 4);
        return buffer.getInt();
    }
}
