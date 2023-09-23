package xiaoyf.demo.kafka.processor;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ScheduledProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    private final Duration interval;
    private final Map<String, List<String>> window;
    private Cancellable initialSchedule;

    public ScheduledProcessor(Duration interval) {
        this.interval = interval;
        this.window = new HashMap<>();
        this.initialSchedule = null;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;

        log.info("Initialising  processor");

        final Duration deltaToNextAlignedSchedule = timeToNextIntervalCheckpoint(System.currentTimeMillis());

        this.initialSchedule = this.context.schedule(
                deltaToNextAlignedSchedule,
                PunctuationType.WALL_CLOCK_TIME,
                this::initialiseSchedule);
    }

    Duration timeToNextIntervalCheckpoint(Long now) {
        final long remainMs = now % interval.toMillis();
        return (remainMs == 0)
                ? interval
                : interval.minusMillis(remainMs);
    }

    @Override
    public void process(Record<String, String> record) {
        window.putIfAbsent(record.key(), new ArrayList<>());
        window.get(record.key()).add(record.value());
    }

    @Override
    public void close() {
        Processor.super.close();
    }

    @SneakyThrows
    void initialiseSchedule(Long now){
        flush(now);

        if (this.initialSchedule != null) {
            this.initialSchedule.cancel();
        }

        this.context.schedule(
                this.interval,
                PunctuationType.WALL_CLOCK_TIME,
                this::flush);
    }

    @SneakyThrows
    void flush(Long now){
        for (Map.Entry<String,List<String>> entry : window.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            this.context.forward(
                    new Record<>(
                            entry.getKey(),
                            String.join(",", entry.getValue()),
                            now
                    )
            );
            this.context.commit();

            log.info("flushed {}, {}", entry.getKey(), String.join(",", entry.getValue()));
            entry.getValue().clear();
        }
    }

}
