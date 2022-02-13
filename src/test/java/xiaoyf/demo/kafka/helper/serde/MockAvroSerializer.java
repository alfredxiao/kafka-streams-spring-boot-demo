package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class MockAvroSerializer extends KafkaAvroSerializer {

    public MockAvroSerializer() {
        super.autoRegisterSchema = true;
        super.schemaRegistry = SingletonMockSchemaRegistryClient.getInstance();
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        log.info("!!! @{} .serialize() topic: {}, class: {}, record:{}",
                this.hashCode(), topic, record.getClass().getName(), record);

        byte[] bytes = super.serialize(topic, record);
        log.info("!!! serialized as bytes: {}, id: {}", bytes.length, readId(bytes));
        return bytes;
    }

    private int readId(byte[] bytes) {
        if (bytes.length < 5) {
            // this is not Avro bytes containing id
            return -2;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes, 1, 4);
        return buffer.getInt();
    }
}
