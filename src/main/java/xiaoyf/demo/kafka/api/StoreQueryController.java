package xiaoyf.demo.kafka.api;


import demo.model.CustomerKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;

import static xiaoyf.demo.kafka.topology.dualjoin.bystore.PreferenceJoiningProcessor.PREFERENCE_STORE;

@RestController
@ConditionalOnProperty(
        prefix="demo-streams",
        name="dual-join-app-enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
@Slf4j
public class StoreQueryController {

    @Autowired
    @Qualifier("&dualJoinStreamsBuilder")
    private final StreamsBuilderFactoryBean factoryBean;

    private final Serde<CustomerKey> customerKeySerde;

    @Value("${server.host}")
    private String advertisedHost;

    @Value("${server.port}")
    private int advertisedPort;

    @GetMapping("/preference/{customerNumber}")
    public PreferenceValue preference(@PathVariable(value = "customerNumber") Long customerNumber) {
        if (factoryBean == null) {
            log.warn("cannot find dualJoinStreamsBuilder bean");
            return null;
        }

        final KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            log.warn("cannot find KafkaStreams instance dualJoinStreamsBuilder");
            return null;
        }

        final CustomerKey key = CustomerKey.newBuilder().setCustomerNumber(customerNumber).build();

        final KeyQueryMetadata metadata = streams.queryMetadataForKey(PREFERENCE_STORE, key, customerKeySerde.serializer());
        if (metadata == null) {
            log.warn("cannot find metadata for key {}", key);
            return null;
        }

        final HostInfo hostInfo = metadata.activeHost();
        log.info("customerNumber={}, hostInfo={}", customerNumber, hostInfo);

        final boolean local = isLocal(hostInfo);
        if (local) {
            final ReadOnlyKeyValueStore<CustomerKey, PreferenceValue> prefStore = streams
                    .store(StoreQueryParameters.fromNameAndType(PREFERENCE_STORE, QueryableStoreTypes.keyValueStore()));

            final PreferenceValue pref = prefStore.get(key);

            if (pref == null) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "preference not found"
                );
            }

            log.info("found preference by customerNumber {}, @ {}:{}",
                    customerNumber, advertisedHost, advertisedPort
            );
            return pref;
        }

        String url = String.format("http://%s:%d/preference/%d",
                hostInfo.host(), hostInfo.port(), customerNumber);

        log.info("non local preference, requesting remote host via url: {}", url);

        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.getForObject(url, PreferenceValue.class);
    }

    private boolean isLocal(HostInfo hostInfo) {
        return hostInfo.host().equals(advertisedHost)
                && hostInfo.port() == advertisedPort;
    }
}