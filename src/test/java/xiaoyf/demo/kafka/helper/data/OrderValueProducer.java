package xiaoyf.demo.kafka.helper.data;


import static xiaoyf.demo.kafka.helper.data.TestData.testOrderKey;
import static xiaoyf.demo.kafka.helper.data.TestData.testOrderValue;

public class OrderValueProducer {
	public static void main(String[] args) throws Exception {
		AnyProducer.produce(
				"order",
				testOrderKey(),
				testOrderValue()
			);

	}
}

/*
 When producing to a normal topic (record timestamp defaulted to CreateTime), record timestamp is defined by producer.
 When producing to a topic set to LogAppendTime, record timestamp is NOT defined by producer
 */