# Background

Id Shortener came from a use case where there is a Kafka topic dedicated for a message which contains
customer IDs. A stream processor reads in these messages (the IDs) into a Set thus gives a total 
count of unique IDs. However, due to the potential size of the Set is high and the length of individual ID is 
of certain number, the end result is that the Set is large in terms of how many bytes it cost. This 
leads to a capacity problem where Kafka by default allows only up to 1M of maximum message size while 
Set of IDs is persisted into a Kafka topic, it can easily hit the up limit.

One solution is to have producer compress the message before handing to Kafka (because the 1M message size limit
is checked before compression done by Kafka - if enabled). The alternative is to replace long string
IDs with a shorter value which uniquely corresponds to the long string ID, e.g. an Integer number.
There maybe other ways such as what URL shortener does. Here we explore the approach where a long
string ID replaced by an Integer. E.g. "c39698fc-4513-4511-b102-384f799a2deb" -> 123.

With Avro encoding, integer is encoded in ZigZag manner, which appears to be more cost-effective 
especially when the number is a low number in integer range.