package xiaoyf.demo.kafka.helper;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.GitProperties;
import xiaoyf.demo.kafka.config.DemoProperties;

@RequiredArgsConstructor
public class PropertiesLogHelper {

    private final DemoProperties properties;
    private final GitProperties gitProperties;

    public void logProperties(org.slf4j.Logger log) {

        log.info("Processor Config: {}", properties);
        log.info("git commit: {}", gitProperties == null ? "NULL" : gitProperties.getShortCommitId());

    }
}
