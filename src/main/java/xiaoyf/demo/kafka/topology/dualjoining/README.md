# Background

This is for dealing the use case where there is a need to join two streams/tables while emit an output
whenever either side of the two sources has changes coming in. E.g. Joining a topic of customer address
and a topic of customer preferences, whenever address or preferences changes, we emit a joining result
for the customer.

With two built-in mechanisms, only table-table join and windowed stream-stream join can deliver such
semantics. 

Custom idea to implement a stream-stream non windowed dual joining:

Settings:

|        | prefStream | addrStream |
|--------|------------|------------|
| store  | addrStore  | perfStore  |
| output | perf+addr  | addr+perf  |

Challenges:
1. Co-partitioning: 
   1. if both perf and addr streams are keyed on the same id, e.g. customer id, then 
   no problem. If not, repartition may be required.
   2. if perf and addr are keyed differently, and there is a FK concept, let's say, perfStream
   has a addrId as FK.
      1. Neither stream is able to lookup corresponding records in its local store, due to they are 
      keyed differently. e.g. perfStream keyed on custoemrId, addrStream keyed on addrId.
      2. if addrStream sends a copy rekeyed on customerId, then perfStream can find it in its store, but
      addrStream does not have data of customerId
      3. Instead, perfStream can send a copy keyed on addrId, which enters perfStore which can then
      be found by addrStream.

|        | prefStream | addrPrefStream | addrStream |
|--------|------------|----------------|------------|
| store  | addrStore  | addrStore      | prefStore  |
| output | pref+addr  | perf+addr      | addr+pref  |

2. Race Condition
   1. no guarantee which one side gets processed first when two records come in the same time
   2. but because this is dual join, a latter processing will emit a joined result anyway
      1. downstream may need a dedupe or delayed processing to reduce events sent to further processing
