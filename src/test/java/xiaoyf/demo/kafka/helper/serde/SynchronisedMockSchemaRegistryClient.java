package xiaoyf.demo.kafka.helper.serde;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * SingletonMockSchemaRegistryClient makes several public methods from MockSchemaRegistryClient 'synchronized'
 * with the hope that it is thread-safe and can be shared between threads in tests. In fact, current test
 * all single threaded so far, and this class is implemented in a naive way, and it is probably not easy
 * to really make it thread-safe otherwise parent class MockSchemaRegistryClient would have done this.
 */
public class SynchronisedMockSchemaRegistryClient extends MockSchemaRegistryClient {

    private static final SynchronisedMockSchemaRegistryClient INSTANCE = new SynchronisedMockSchemaRegistryClient();

    private SynchronisedMockSchemaRegistryClient() {
    }

    public static SynchronisedMockSchemaRegistryClient getInstance() {
        return INSTANCE;
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema)
            throws IOException, RestClientException {
        return super.register(subject, schema);
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException {
        return super.register(subject, schema, normalize);
    }

    @Override
    public synchronized int register(String subject, ParsedSchema schema, int version, int id)
            throws IOException, RestClientException {
        return super.register(subject, schema, version, id);
    }

    @Override
    public synchronized ParsedSchema getSchemaById(int id) throws IOException, RestClientException {
        return super.getSchemaById(id);
    }

    @Override
    public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        return super.getSchemaBySubjectAndId(subject, id);
    }

    @Override
    public synchronized int getId(String subject, ParsedSchema schema) throws IOException, RestClientException {
        return super.getId(subject, schema);
    }

    @Override
    public synchronized int getId(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException {
        return super.getId(subject, schema, normalize);
    }

    @Override
    public synchronized Optional<ParsedSchema> parseSchema(
            String schemaType,
            String schemaString,
            List<SchemaReference> references) {
        return super.parseSchema(schemaType, schemaString, references);
    }
    @Override
    public synchronized List<ParsedSchema> getSchemas(
            String subjectPrefix,
            boolean lookupDeletedSchema,
            boolean latestOnly)
            throws IOException, RestClientException {
        return super.getSchemas(subjectPrefix, lookupDeletedSchema, latestOnly);
    }

    @Override
    public synchronized Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException {
        return super.getAllSubjectsById(id);
    }

    @Override
    public synchronized Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
            RestClientException {
        return super.getAllVersionsById(id);
    }

    @Override
    public synchronized Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
        return super.getByVersion(subject, version, lookupDeletedSchema);
    }

    @Override
    public synchronized SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
        return super.getSchemaMetadata(subject, version);
    }

    @Override
    public synchronized SchemaMetadata getLatestSchemaMetadata(String subject)
            throws IOException, RestClientException {
        return super.getLatestSchemaMetadata(subject);
    }

    @Override
    public synchronized int getVersion(String subject, ParsedSchema schema)
            throws IOException, RestClientException {
        return super.getVersion(subject, schema);
    }

    @Override
    public synchronized int getVersion(String subject, ParsedSchema schema, boolean normalize)
            throws IOException, RestClientException {
        return super.getVersion(subject, schema, normalize);
    }

    @Override
    public synchronized List<Integer> getAllVersions(String subject)
            throws IOException, RestClientException {
        return super.getAllVersions(subject);
    }

    @Override
    public synchronized boolean testCompatibility(String subject, ParsedSchema newSchema) throws IOException,
            RestClientException {
        return super.testCompatibility(subject, newSchema);
    }

    @Override
    public synchronized String getCompatibility(String subject) throws IOException, RestClientException {
        return super.getCompatibility(subject);
    }

    @Override
    public synchronized String setMode(String mode)
            throws IOException, RestClientException {
        return super.setMode(mode);
    }

    @Override
    public synchronized String setMode(String mode, String subject)
            throws IOException, RestClientException {
        return super.setMode(mode, subject);
    }

    @Override
    public synchronized String getMode() throws IOException, RestClientException {
        return super.getMode();
    }

    @Override
    public synchronized String getMode(String subject) throws IOException, RestClientException {
        return super.getMode(subject);
    }

    @Override
    public synchronized Collection<String> getAllSubjects() throws IOException, RestClientException {
        return super.getAllSubjects();
    }

    @Override
    public synchronized Collection<String> getAllSubjectsByPrefix(String subjectPrefix)
            throws IOException, RestClientException {
        return super.getAllSubjectsByPrefix(subjectPrefix);
    }
}
