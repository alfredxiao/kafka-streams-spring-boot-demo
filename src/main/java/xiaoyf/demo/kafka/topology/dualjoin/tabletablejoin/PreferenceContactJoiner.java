package xiaoyf.demo.kafka.topology.dualjoin.tabletablejoin;

import demo.model.ContactValue;
import demo.model.EnrichedPreferenceValue;
import demo.model.PreferenceValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="table-table-join-app-enabled",
        havingValue = "true"
)
public class PreferenceContactJoiner implements ValueJoiner<PreferenceValue, ContactValue, EnrichedPreferenceValue> {
    @Override
    public EnrichedPreferenceValue apply(PreferenceValue pref, ContactValue contact) {
        return EnrichedPreferenceValue.newBuilder()
                .setPreferredComms(pref.getPreferredComms())
                .setCustomerNumber(pref.getCustomerNumber())
                .setContact(contact)
                .build();
    }
}
