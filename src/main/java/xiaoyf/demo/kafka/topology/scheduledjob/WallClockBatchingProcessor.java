package xiaoyf.demo.kafka.topology.scheduledjob;

import demo.model.CustomerOrderBatchKey;
import demo.model.CustomerOrderBatchValue;
import demo.model.OrderKey;
import demo.model.OrderValue;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 WallClockWindowingProcessor mimics a wall-clock based tumbling windowing. With 5 minutes window size, it emits
 order list of [0,5), [5,10), etc. where its boundaries is wall-clock time, not timestamps from input records.
 */
@Slf4j
public class WallClockBatchingProcessor implements Processor<OrderKey, OrderValue, CustomerOrderBatchKey, CustomerOrderBatchValue> {

    private ProcessorContext<CustomerOrderBatchKey, CustomerOrderBatchValue> context;
    private final Duration interval;
    private final Map<CustomerOrderBatchKey, List<OrderValue>> batches;
    private Cancellable initialSchedule;

    public WallClockBatchingProcessor(Duration interval) {
        this.interval = interval;
        this.batches = new HashMap<>();
        this.initialSchedule = null;
    }

    @Override
    public void init(ProcessorContext<CustomerOrderBatchKey, CustomerOrderBatchValue> context) {
        this.context = context;
        log.info("Initialising scheduled job process, interval={}", this.interval);

        final long current = System.currentTimeMillis();//context.currentSystemTimeMs();
        final long windowMs = interval.toMillis();
        final long remainMs = windowMs - (current % interval.toMillis());
        log.info("time to next run: {} ms, curr:{}, interval:{}", remainMs, current, interval.toMillis());

        this.initialSchedule = this.context.schedule(
                Duration.ofMillis(remainMs),
                PunctuationType.WALL_CLOCK_TIME,
                this::firstRun);
    }

    @SneakyThrows
    void firstRun(Long now) {
        log.info("book regular schedule...");

        flush(now);

        if (this.initialSchedule != null) {
            this.initialSchedule.cancel();
        }

        this.context.schedule(
                this.interval,
                PunctuationType.WALL_CLOCK_TIME,
                this::flush);
    }

    @Override
    public void process(Record<OrderKey, OrderValue> record) {
        log.info("processing record: {}", record);
        if (record.value() == null || record.value().getCustomerNumber() == null) {
            log.warn("order {} does not have customer number, ignore", record.key().getOrderNumber());
            return;
        }

        final long now = context.currentSystemTimeMs();
        final TimeWindow timeWindow = TimeWindow.windowFor(now, interval.toMillis());

        final CustomerOrderBatchKey key = batchKey(record.value().getCustomerNumber(), timeWindow);

        batches.putIfAbsent(key, new ArrayList<>());
        batches.get(key).add(record.value());
    }

    @Override
    public void close() {
        Processor.super.close();
    }

    @SneakyThrows
    void flush(Long now){
        log.info("flush running on {}", now);
        for (Map.Entry<CustomerOrderBatchKey,List<OrderValue>> entry : batches.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            this.context.forward(
                    new Record<>(
                            entry.getKey(),
                            batchValue(
                                    entry.getKey(),
                                    entry.getValue()),
                            now
                    )
            );
            this.context.commit();

            log.info("flushed/committed customerNumber: {}, order count: {}", entry.getKey(), entry.getValue().size());
            entry.getValue().clear();
        }
    }

    private CustomerOrderBatchKey batchKey(final Long customerNumber, TimeWindow timeWindow) {
        return CustomerOrderBatchKey.newBuilder()
                .setCustomerNumber(customerNumber)
                .setWinStart(Instant.ofEpochMilli(timeWindow.start()))
                .setWinEnd(Instant.ofEpochMilli(timeWindow.end()))
                .build();
    }
    private CustomerOrderBatchValue batchValue(
            final CustomerOrderBatchKey key,
            final List<OrderValue> orders) {

        return CustomerOrderBatchValue.newBuilder()
                .setCustomerNumber(key.getCustomerNumber())
                .setWinStart(key.getWinStart())
                .setWinEnd(key.getWinEnd())
                .setOrders(orders)
                .build();
    }
}
