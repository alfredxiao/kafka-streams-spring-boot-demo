package xiaoyf.demo.kafka.topology.scheduledjob;

import lombok.experimental.UtilityClass;

import java.time.Instant;

@UtilityClass
public class DateUtils {
    public static String format(final Long epoch) {
        if (epoch == null) {
            return "";
        }

        return Instant.ofEpochMilli(epoch).toString();
    }
}
