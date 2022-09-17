package xiaoyf.demo.kafka.joiner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import static java.lang.String.format;

@Component
@Slf4j
public class ClickLocationValueJoiner implements ValueJoiner<String, String, String> {
    @Override
    public String apply(String click, String location) {
        return format("%s accessed from %s", click, location);
    }
}
