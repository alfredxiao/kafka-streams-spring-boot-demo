package xiaoyf.demo.kafka.topology.selfjoin;

import demo.model.CommunicationChannel;
import demo.model.PreferenceChange;
import demo.model.PreferenceValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import static demo.model.CommunicationChannel.EMAIL;
import static demo.model.CommunicationChannel.SNAIL;

@Component
@Slf4j
public class PreferenceSelfJoiner implements ValueJoiner<PreferenceValue, PreferenceValue, PreferenceChange>{
    @Override
    public PreferenceChange apply(PreferenceValue prev, PreferenceValue curr) {
        log.info("prev={}, cur={}", prev, curr);
        return PreferenceChange.newBuilder()
                .setCustomerNumber(curr.getCustomerNumber())
                .setPrevious(prev.getPreferredComms())
                .setCurrent(curr.getPreferredComms())
                .setGreenChange(isGreenChange(prev.getPreferredComms(), curr.getPreferredComms()))
                .build();
    }

    private boolean isGreenChange(CommunicationChannel prev, CommunicationChannel curr) {
        return prev == SNAIL && curr == EMAIL;
    }
}
