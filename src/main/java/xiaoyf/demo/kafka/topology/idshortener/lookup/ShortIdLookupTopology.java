package xiaoyf.demo.kafka.topology.idshortener.lookup;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Qualifier;

/*
  ShortIdLookupTopology handles input records with long id, by looking up short ids generated by
  ShortIdRegistryTopology, then replacing long id with short id.
  Purpose is to make message size smaller, such that when tens of thousands of small records are
  aggregated into a large message, the message size is kept low. (Note default Kafka message size
  limit is 1M).
 */
public class ShortIdLookupTopology {
    void process(@Qualifier("shortIdLookupStreamsBuilder") StreamsBuilder builder) {
//        builder.stream("message_with_long_id")
//                .map(pick_just_id)
//                .to("long_id_topic"); // kick off work in registry topology
//
//        KTable idTable = builder.table("long_id_to_short_id"); // the store from registry topology
//        builder.stream("message_with_long_id")
//                .toTable("by_key")
//                .join(idTable)
//                .to("message_with_short_id");
    }
}
