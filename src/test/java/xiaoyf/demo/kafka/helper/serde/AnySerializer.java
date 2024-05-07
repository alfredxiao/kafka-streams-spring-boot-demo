package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

import static xiaoyf.demo.kafka.helper.serde.AvroUtils.MOCK_CONFIG;
import static xiaoyf.demo.kafka.helper.serde.AvroUtils.isAvroRecord;

public class AnySerializer implements Serializer<Object> {

    private boolean isKey;
    private final KafkaAvroSerializer avroSerializer;
    private final PrimitiveSerdesRegistry primitiveSerdesRegistry = PrimitiveSerdesRegistry.getInstance();

    public AnySerializer() {
        this.avroSerializer = new KafkaAvroSerializer();
    }

    public AnySerializer(boolean isKey) {
        this.isKey = isKey;
        this.avroSerializer = new KafkaAvroSerializer();
        this.avroSerializer.configure(MOCK_CONFIG, isKey);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.avroSerializer.configure(MOCK_CONFIG, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (Objects.isNull(data)) {
            return null;
        }

        if (isAvroRecord(data)) {
            return avroSerializer.serialize(topic, data);
        }

        try (Serializer<Object> s = primitiveSerdesRegistry.serializer(topic, isKey, data.getClass())) {
            return s.serialize(topic, data);
        }
    }
}
