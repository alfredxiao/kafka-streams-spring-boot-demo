package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;

@Slf4j
public class MockSerializer extends KafkaAvroSerializer {

    private final SingletonNonAvroRegistry nonAvroRegistry = SingletonNonAvroRegistry.getInstance();

    public MockSerializer() {
        super.autoRegisterSchema = true;
        super.schemaRegistry = SingletonMockSchemaRegistryClient.getInstance();
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        log.info("!!! @{} .serialize() topic: {}, class: {}, record:{}",
                this.hashCode(), topic, record.getClass().getName(), record);

        if (isAvro(record)) {
            byte[] bytes = super.serialize(topic, record);
            log.info("!!! serialized as bytes: {}, id: {}", bytes.length, readId(bytes));
            return bytes;
        }

        return nonAvroRegistry.getNonAvroSerializer(topic, record.getClass()).serialize(topic, record);
    }

    private int readId(byte[] bytes) {
        if (bytes.length < 5) {
            // this is not Avro bytes containing id
            return -2;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes, 1, 4);
        return buffer.getInt();
    }

    private boolean isAvro(Object record) {
        return record instanceof SpecificRecord || record instanceof GenericRecord;
    }
}
