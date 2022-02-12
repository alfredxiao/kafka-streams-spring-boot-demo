package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class SharedMockSchemaRegistryClient extends MockSchemaRegistryClient {

    private static final SharedMockSchemaRegistryClient INSTANCE = new SharedMockSchemaRegistryClient();

    private static final Object LOCK = new Object();

    private SharedMockSchemaRegistryClient() {
    }

    public static SharedMockSchemaRegistryClient getInstance() {
        return INSTANCE;
    }

    @Override
    public int register(String subject, ParsedSchema schema)
            throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.register(subject, schema);
        }
    }

    @Override
    public int register(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.register(subject, schema, normalize);
        }
    }

    @Override
    public int register(String subject, ParsedSchema schema, int version, int id)
            throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.register(subject, schema, version, id);
        }
    }

    @Override
    public ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.getSchemaById(id);
        }
    }

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.getSchemaBySubjectAndId(subject, id);
        }
    }

    @Override
    public int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.getId(subject, schema);
        }
    }

    @Override
    public int getId(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException {
        synchronized (LOCK) {
            return super.getId(subject, schema, normalize);
        }
    }
}
