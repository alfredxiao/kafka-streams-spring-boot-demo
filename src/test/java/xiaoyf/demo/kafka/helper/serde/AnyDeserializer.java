package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

import static xiaoyf.demo.kafka.helper.serde.AvroUtils.MOCK_CONFIG;

public class AnyDeserializer implements Deserializer<Object> {

    private boolean isKey;
    private KafkaAvroDeserializer avroDeserializer;
    private final PrimitiveSerdesRegistry primitiveSerdesRegistry = PrimitiveSerdesRegistry.getInstance();

    public AnyDeserializer() {
        this.avroDeserializer = new KafkaAvroDeserializer();
    }

    public AnyDeserializer(boolean isKey) {
        this.isKey = isKey;
        this.avroDeserializer = new KafkaAvroDeserializer();
        this.avroDeserializer.configure(MOCK_CONFIG, isKey);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.avroDeserializer.configure(MOCK_CONFIG, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        Deserializer<Object> deserializer = this.primitiveSerdesRegistry.deserializer(topic, isKey);

        if (deserializer != null) {
            return deserializer.deserialize(topic, data);
        }

        return this.avroDeserializer.deserialize(topic, data);
    }
}
