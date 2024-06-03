package xiaoyf.demo.kafka.helper.serde;

import lombok.experimental.UtilityClass;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

@UtilityClass
public class AvroUtils {
    public static final Map<String, Object> MOCK_CONFIG = Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy",
            SPECIFIC_AVRO_READER_CONFIG, true,
            AUTO_REGISTER_SCHEMAS, true
    );

    public static boolean isAvroRecord(Object record) {
        return record instanceof SpecificRecord || record instanceof GenericRecord;
    }

    public static int readId(byte[] bytes) {
        if (bytes.length < 5 || bytes[0] != 0) {
            // this is not Avro bytes
            return -2;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes, 1, 4);
        return buffer.getInt();
    }
}
