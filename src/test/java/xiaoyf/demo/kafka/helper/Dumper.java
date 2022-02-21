package xiaoyf.demo.kafka.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.springframework.context.ApplicationContext;
import xiaoyf.demo.kafka.helper.serde.SingletonMockSchemaRegistryClient;

import java.util.Set;

import static java.lang.String.format;

@Slf4j
public class Dumper {

    public static void dumpAllBeans(final ApplicationContext applicationContext) {
        String[] allBeanNames = applicationContext.getBeanDefinitionNames();
        for(String beanName : allBeanNames) {
            var bean = applicationContext.getBean(beanName);

            log.info("Bean name: {}, class: {}, hashCode: {}", beanName, bean.getClass().getName(), bean.hashCode());
        }
    }

    public static void dumpTopology(final String name, final Topology topology) {
        log.info("Topology {}", name);
        log.info(topology.describe().toString());

        TopologyDescription description = topology.describe();

        Set<TopologyDescription.GlobalStore> stores = description.globalStores();
        stores.forEach(store -> {
            log.info("GlobalStore: class: {}", store.getClass().getName());
        });
    }

    public static void dumpTestDriverStats(final TopologyTestDriver driver) {

        log.info("=== produced topic names:");
        driver.producedTopicNames().forEach(log::info);

        log.info("=== all state stores:");
        driver.getAllStateStores().forEach((name, store) -> {
            log.info("name: {}, class: {}, persistent: {}, isOpen: {}",
                    name, store.getClass(), store.persistent(), store.isOpen());
        });
    }

    public static void dumpTopicAndSchemaList() throws Exception {
        log.info("!!! SUBJECT LIST");
        SingletonMockSchemaRegistryClient.getInstance().getAllSubjects()
                .stream()
                .sorted()
                .forEach(subject -> {
                    try {
                        var meta = SingletonMockSchemaRegistryClient.getInstance().getLatestSchemaMetadata(subject);
                        log.info("!!! subject={}, id={}, schema={}", subject, meta.getId(), meta.getSchema());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

    }
}
