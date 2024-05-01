package xiaoyf.demo.kafka.helper.testhelper;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import xiaoyf.demo.kafka.commons.config.GenericSerdeFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProcessorTestHelper<KI, VI, KS extends SpecificRecord, VS extends SpecificRecord,  KO, VO> {
    private final MockProcessorContext<KO, VO> context;

    @Getter
    private final KeyValueStore<KS, VS> store;
    public ProcessorTestHelper(Processor<KI, VI, KO, VO> processor, String storeName) {

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        context = new MockProcessorContext<>(config);

        Map<String, Object> conf = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy"
        );
        GenericSerdeFactory factory = new GenericSerdeFactory(conf);

        store = Stores
                .keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        factory.<KS>keySerde(),
                        factory.<VS>valueSerde())
                .withLoggingDisabled().build();
        store.init(context.getStateStoreContext(), store);
        context.addStateStore(store);

        processor.init(context);
    }

    public List<MockProcessorContext.CapturedForward<? extends KO, ? extends VO>> getForwarded() {
        return context.forwarded();
    }

}
