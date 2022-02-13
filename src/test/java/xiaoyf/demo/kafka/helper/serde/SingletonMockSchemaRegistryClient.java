package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

public class SingletonMockSchemaRegistryClient {

    private static final MockSchemaRegistryClient INSTANCE = new MockSchemaRegistryClient();

    private SingletonMockSchemaRegistryClient() {
    }

    public static MockSchemaRegistryClient getInstance() {
        return INSTANCE;
    }
}
