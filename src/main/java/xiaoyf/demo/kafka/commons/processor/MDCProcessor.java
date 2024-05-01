package xiaoyf.demo.kafka.commons.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.MDC;

@Slf4j
public class MDCProcessor<K, V> extends ContextualProcessor<K, V, K, V> {
    private ProcessorContext<K, V> context;

    @Override
    public void process(Record<K, V> record) {
        context.recordMetadata().ifPresent(meta -> {
            MDC.put("kafkaMeta", String.format("[%s-%d-%d]", meta.topic(), meta.partition(), meta.offset()));
        });
        context.forward(record);
    }

    @Override
    public void init(final ProcessorContext<K, V> myContext) {
        this.context = myContext;
    }
}
