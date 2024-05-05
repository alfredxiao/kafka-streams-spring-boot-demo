package xiaoyf.demo.kafka.topology.fklookup.commons;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderEnriched;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import static xiaoyf.demo.kafka.topology.fklookup.commons.EnrichUtils.enrich;

/**
 * FkLookupProcessor applies generic foreign key lookup using a global store.
 */
@Slf4j
public class CustomerStoreLookupProcessor implements Processor<OrderKey, OrderValue, OrderKey, OrderEnriched> {
    public static final String CUSTOMER_STORE = "customer-store";
    private ProcessorContext<OrderKey, OrderEnriched> context;
    protected ReadOnlyKeyValueStore<CustomerKey, CustomerValue> store;

    @Override
    public void init(ProcessorContext<OrderKey, OrderEnriched> context) {
        this.context = context;
        this.store = context.getStateStore(CUSTOMER_STORE);
    }

    @Override
    public void process(Record<OrderKey, OrderValue> record) {
        try {
            final Long orderNumber = record.key().getOrderNumber();
            if (record.value() == null) {
                log.info("order value is null, ignoring foreign key lookup, order no.: {}", orderNumber);
                return;
            }

            final Long customerNumber = record.value().getCustomerNumber();
            if (customerNumber == null) {
                log.info("no foreign key extracted, order no.: {}", orderNumber);
                return;
            }

            final CustomerKey customerKey = CustomerKey.newBuilder().setCustomerNumber(customerNumber).build();
            final CustomerValue customer = store.get(customerKey);
            if (customer == null) {
                log.info("foreign key extracted: {}, but no corresponding record found via lookup, order no. : {}",
                        customerNumber, orderNumber);
                return;
            }

            log.info("lookup found foreign record for order no.: {}, customer no.: {}",
                    orderNumber, customerNumber);

            context.forward(new Record<>(
                    record.key(),
                    enrich(record.value(), customer),
                    record.timestamp()
            ));
        } catch (Exception ex) {
            log.error("Failed to process inbound message", ex);
        }

    }

}
