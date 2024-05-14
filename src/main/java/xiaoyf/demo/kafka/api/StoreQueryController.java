package xiaoyf.demo.kafka.api;


import demo.model.CustomerKey;
import demo.model.PreferenceValue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import xiaoyf.demo.kafka.helper.StoreUtils;

import java.util.List;
import java.util.Map;

import static xiaoyf.demo.kafka.topology.dualjoin.PreferenceJoiningProcessor.PREFERENCE_STORE;

@RestController
@RequiredArgsConstructor
@Slf4j
public class StoreQueryController {

    @Autowired
    @Qualifier("&dualJoinStreamsBuilder")
    private StreamsBuilderFactoryBean factoryBean;


    @GetMapping("/preference/{customerNumber}")
    public PreferenceValue preference(@PathVariable(value = "customerNumber") Long preferenceNumber) {

        final KafkaStreams streams = factoryBean.getKafkaStreams();
        if (streams == null) {
            log.warn("cannot find KafkaStreams instance dualJoinStreamsBuilder");
            return null;
        }

        final ReadOnlyKeyValueStore<CustomerKey, PreferenceValue> prefStore = streams
                    .store(StoreQueryParameters.fromNameAndType(PREFERENCE_STORE, QueryableStoreTypes.keyValueStore()));

        final PreferenceValue pref = prefStore.get(
                CustomerKey.newBuilder().setCustomerNumber(preferenceNumber).build()
        );

        if (pref == null) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, "preference not found"
            );
        }

        return pref;
    }
}