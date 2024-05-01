package xiaoyf.demo.kafka.commons.processor;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ProcessorUtils {


    public <V> boolean isDuplicated(final V existing, final V newValue) {
        if (existing == null && newValue == null) {
            return true;
        }

        if (existing == null) {
            return false;
        }

        return existing.equals(newValue);
    }
}
