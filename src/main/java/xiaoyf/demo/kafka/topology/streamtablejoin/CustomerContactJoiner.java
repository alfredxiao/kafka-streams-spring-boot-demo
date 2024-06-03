package xiaoyf.demo.kafka.topology.streamtablejoin;

import demo.model.ContactValue;
import demo.model.CustomerValue;
import demo.model.EnrichedCustomerValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

@Component
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
