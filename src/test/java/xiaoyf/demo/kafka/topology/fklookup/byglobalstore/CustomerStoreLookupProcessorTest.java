package xiaoyf.demo.kafka.topology.fklookup.byglobalstore;

import demo.model.CustomerKey;
import demo.model.CustomerValue;
import demo.model.OrderEnriched;
import demo.model.OrderKey;
import demo.model.OrderValue;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xiaoyf.demo.kafka.helper.testhelper.ProcessorTestHelper;
import xiaoyf.demo.kafka.topology.fklookup.commons.CustomerStoreLookupProcessor;

import static org.assertj.core.api.Assertions.assertThat;
import static xiaoyf.demo.kafka.helper.data.TestData.CUSTOMER_NAME;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testCustomerValue;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;
import static xiaoyf.demo.kafka.topology.fklookup.commons.CustomerStoreLookupProcessor.CUSTOMER_STORE;

public class CustomerStoreLookupProcessorTest {

    private CustomerStoreLookupProcessor processor;
    private ProcessorTestHelper<OrderKey, OrderValue, CustomerKey, CustomerValue, OrderKey, OrderEnriched> helper;

    @BeforeEach
    public void init() {
        processor = new CustomerStoreLookupProcessor();
        helper = new ProcessorTestHelper<>(processor, CUSTOMER_STORE);
    }

    @Test
    public void shouldLookupCustomer() {
        helper.getStore().put(testCustomerKey(), testCustomerValue());

        Record<OrderKey, OrderValue> record = new Record<>(
                testOrderKey(),
                testOrderValue(),
                1L
        );
        processor.process(record);

        final var forwarded = helper.getForwarded();

        assertThat(forwarded).hasSize(1);
        assertThat(forwarded.get(0).record().value()).isNotNull();
        assertThat(forwarded.get(0).record().value().getCustomer()).isNotNull();
        assertThat(forwarded.get(0).record().value().getCustomer().getName()).isEqualTo(CUSTOMER_NAME);
    }

}
