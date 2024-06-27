demonstrates how schema id is used (in keys of avro type)

steps
1. generates contact with key v1 (results in v1 contact in contact-table)
2. modify customer key definition with slight change (like avro.type.string=java on a type, or myversion=2 on top level)
3. generates customer with key v2 (results in failed lookup - even if partition count is 1 where all records routed to same partition)
4. this is because a new schema id is used and it is part of the key bytes (avro encoded bytes) that starts with schema id

The same can happen with custom stores as long as keys are in avro. The observed behavior is that
Observed: store.get(key) returns null, but converting store.all() to HashMap and you can see key is there.
Reason: the 'key' stored is a sequence of bytes starting with schema id, if the 'key' you provide happens to use a 
        newer or older schema id, the lookup will fail regardless of 'key' equals (in Java sense)