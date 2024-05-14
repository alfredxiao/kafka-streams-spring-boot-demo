package xiaoyf.demo.kafka.helper.data;

public class DualJoinProducer {
    public static void main(String[] args) throws Exception {
        AnyProducer.produce(
                "preference",
                TestData.testPreferenceKey(),
                TestData.testPreferenceValue()
        );
    }
}
