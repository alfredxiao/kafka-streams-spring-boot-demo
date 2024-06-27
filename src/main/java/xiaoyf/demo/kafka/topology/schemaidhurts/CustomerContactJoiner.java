package xiaoyf.demo.kafka.topology.schemaidhurts;

import demo.model.ContactValue;
import demo.model.CustomerValue;
import demo.model.EnrichedCustomerValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        prefix="demo-streams",
        name="schema-id-hurts-app-enabled",
        havingValue = "true"
)
public class CustomerContactJoiner implements ValueJoiner<CustomerValue, ContactValue, EnrichedCustomerValue>{
    @Override
    public EnrichedCustomerValue apply(CustomerValue customer, ContactValue contact) {
        return EnrichedCustomerValue.newBuilder()
                .setCustomerNumber(customer.getCustomerNumber())
                .setName(customer.getName())
                .setActiveCampaigns(customer.getActiveCampaigns())
                .setContact(contact)
                .build();
    }
}
