package xiaoyf.demo.kafka.helper;

import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serde;

@UtilityClass
public class Serdes {

    public static Serde<String> stringSerde() {
        return org.apache.kafka.common.serialization.Serdes.String();
    }
}
