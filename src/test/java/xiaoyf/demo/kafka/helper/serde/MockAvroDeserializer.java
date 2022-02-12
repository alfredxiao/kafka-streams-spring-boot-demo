package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MockAvroDeserializer extends KafkaAvroDeserializer {

    private static final Map<Pair<String, Boolean>, Schema> SCHEMA_MAP = new HashMap<>();

    public static void registerTopic(final String topic, final boolean isKey, final Schema schema) {
        SCHEMA_MAP.put(Pair.of(topic, isKey), schema);
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        log.info("!!! {} .deserialize() topic:{}, isKey: {}", this, topic, isDeserializingKey());
        Pair<String, Object> entry = Pair.of(topic, isDeserializingKey());

        this.schemaRegistry = SharedMockSchemaRegistryClient.getInstance();//mockSchemaRegistryClient(entry);

        super.useSpecificAvroReader = true;
        Object result = super.deserialize(topic, bytes);

        log.info("!!! {} .deserialize() result class:{}, content:{}", this, result.getClass().getName(), result);

        return result;
    }

//    private SchemaRegistryClient mockSchemaRegistryClient(Pair<String, Object> entry) {
//        return new MockSchemaRegistryClient() {
//            @Override
//            public synchronized ParsedSchema getSchemaById(int id) {
//                return new AvroSchema(SCHEMA_MAP.get(entry));
//            }
//
//            @Override
//            public ParsedSchema getSchemaBySubjectAndId(String subject, int id) {
//                return new AvroSchema(SCHEMA_MAP.get(entry));
//            }
//        };
//    }

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
