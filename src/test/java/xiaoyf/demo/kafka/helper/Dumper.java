package xiaoyf.demo.kafka.helper;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import xiaoyf.demo.kafka.helper.serde.SharedMockSchemaRegistryClient;

import java.util.Arrays;

import static java.lang.String.format;

public class Dumper {

    public static void dumpTopology(Topology topology) {
        log("=== ");
        log(topology.describe());
    }

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

    public static void dumpTopicAndSchemaList() throws Exception {
        log("!!! SUBJECT LIST");
        SharedMockSchemaRegistryClient.getInstance().getAllSubjects()
                .stream()
                .sorted()
                .forEach(subject -> {
                    try {
                        var meta = SharedMockSchemaRegistryClient.getInstance().getLatestSchemaMetadata(subject);
                        log(format("!!! subject=%s, id=%s, schema=%s", subject, meta.getId(), meta.getSchema()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

    }
}
