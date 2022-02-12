package xiaoyf.demo.kafka.helper;

import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Arrays;

public class Dumper {

    public static void dumpTestDriverStats(TopologyTestDriver driver) {

        log("=== produced topic names:");
        driver.producedTopicNames().forEach(Dumper::log);

        log("=== all state stores:");
        driver.getAllStateStores().forEach(Dumper::log);

        // log("=== metrics:");
        // driver.metrics().forEach(Dumper::log);
    }

    public static void log(Object ...obj) {
        Arrays.stream(obj).forEach(e -> System.out.println("  === " + e));
    }
}
