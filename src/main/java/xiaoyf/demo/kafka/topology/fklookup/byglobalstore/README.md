# Comments

Global store is backed by a source topic which is meant to be compacted (otherwise, when application
is restarted, it reads in different set of keys due to some segments are already deleted).

Global store - similar to `GlobalKTble` - is initilised before other streaming tasks are started. As a 
result, records written to the global store's source topic - if they were written before other 
streaming tasks are started - goes into the store such that other streaming tasks can find them. But
if they are written while other tasks are already started, they may be a race condition - meaning, 
for example, two records are being written to Kafka, one record hits the global store's source topic,
the other record hits a topic where a `processe()` task would be looking for a key in the global 
store and does transformation as such; in such a scenario, there maybe a race condition, if the 
`process()` task handling the second record is looking for the key of the first record, no guarantee
that it can find them, depending on which record gets processed first.

Even if memory store is used, serdes are still invoked to convert between Java objects and bytes?
