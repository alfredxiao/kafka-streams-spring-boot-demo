package xiaoyf.demo.kafka.helper.data;

public class DualJoinProducer {
    public static void main(String[] args) throws Exception {
        var pref = TestData.testPreferenceValue();
        pref.setCustomerNumber(2001);
        AnyProducer.produce(
                "preference",
                TestData.testPreferenceKey(),
                pref
        );
    }
}
