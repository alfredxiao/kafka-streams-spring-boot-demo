package xiaoyf.demo.kafka.helper.serde;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrimitiveSerdesRegistry {

    private static final PrimitiveSerdesRegistry INSTANCE = new PrimitiveSerdesRegistry();

    private final Map<String, Class<?>> nonAvroKeyTypes;
    private final Map<String, Class<?>> nonAvroValueTypes;
    private final Map<Class<?>, Serializer<?>> serializerMap;
    private final Map<Class<?>, Deserializer<?>> deserializerMap;

    private PrimitiveSerdesRegistry() {
        this.nonAvroKeyTypes = new ConcurrentHashMap<>();
        this.nonAvroValueTypes = new ConcurrentHashMap<>();
        this.serializerMap = new ConcurrentHashMap<>();
        this.deserializerMap = new ConcurrentHashMap<>();

        this.serializerMap.putAll(Map.of(
                String.class, new StringSerializer(),
                Float.class, new FloatSerializer(),
                Double.class, new DoubleSerializer(),
                Short.class, new ShortSerializer(),
                Integer.class, new IntegerSerializer(),
                Long.class, new LongSerializer(),
                byte[].class, new ByteArraySerializer()
        ));

        this.deserializerMap.putAll(Map.of(
                String.class, new StringDeserializer(),
                Float.class, new FloatDeserializer(),
                Double.class, new DoubleDeserializer(),
                Short.class, new ShortDeserializer(),
                Integer.class, new IntegerDeserializer(),
                Long.class, new LongDeserializer(),
                byte[].class, new ByteArrayDeserializer()
        ));
    }

    public static PrimitiveSerdesRegistry getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    public Serializer<Object> serializer(final String topic, boolean isKey, final Class<?> clazz) {
        nonAvroTypes(isKey).putIfAbsent(topic, clazz);

        return (Serializer<Object>) serializerMap.get(clazz);
    }

    @SuppressWarnings("unchecked")
    public Deserializer<Object> deserializer(final String topic, boolean isKey) {
        Class<?> type = nonAvroTypes(isKey).get(topic);

        if (type == null) return null;

        return (Deserializer<Object>) deserializerMap.get(type);
    }

    private Map<String, Class<?>> nonAvroTypes(boolean isKey) {
        return isKey ? nonAvroKeyTypes : nonAvroValueTypes;
    }
}
