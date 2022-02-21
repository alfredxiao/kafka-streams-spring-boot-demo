package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MockSerde<T> implements Serde<T>, Serializer<T>, Deserializer<T> {
    private final KafkaAvroSerializer serializer;
    private final KafkaAvroDeserializer deserializer;

    public MockSerde() {
        this.serializer = new MockSerializer();
        this.deserializer = new MockDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.serializer.configure(configs, isKey);
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.serializer.close();
        this.deserializer.close();
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return this.serializer.serialize(topic, data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        return (T) this.deserializer.deserialize(topic, data);
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
