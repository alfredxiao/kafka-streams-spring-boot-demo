package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;

@Slf4j
public class MockAvroDeserializer extends KafkaAvroDeserializer {

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        log.info("!!! @{} .deserialize() topic: {}, isKey: {}", this.hashCode(), topic, isDeserializingKey());

        if ("kafka-demo-ORDER-JOINS-CUST-DETAILS-changelog".equals(topic)) {
            int c = 3;
        }
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
}
