package xiaoyf.demo.kafka.helper.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AnySerde <T> implements Serde<T> {
    private AnySerializer anySerializer;
    private AnyDeserializer anyDeserializer;

    public AnySerde() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (anySerializer == null) {
            anySerializer = new AnySerializer(isKey);
            anySerializer.configure(configs, isKey);
        }

        if (anyDeserializer == null) {
            anyDeserializer = new AnyDeserializer(isKey);
            anyDeserializer.configure(configs, isKey);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Serializer<T> serializer() {
        return (Serializer<T>) anySerializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Deserializer<T> deserializer() {
        return (Deserializer<T>) anyDeserializer;
    }
}
