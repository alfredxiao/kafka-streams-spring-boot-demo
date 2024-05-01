package xiaoyf.demo.kafka.commons.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;

@RequiredArgsConstructor
public class GenericSerdeFactory {

    // Kafka properties
    private final Map<String, Object> properties;

    public <T extends SpecificRecord> Serde<T> keySerde() {
        Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(properties, true);
        return serde;
    }

    public <T extends SpecificRecord> Serde<T> valueSerde() {
        Serde<T> serde = new SpecificAvroSerde<>();
        serde.configure(properties, false);
        return serde;
    }
}
