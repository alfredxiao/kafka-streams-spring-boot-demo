# Introduction
This repository aims to provide some working code examples for spring boot based kafka streams applications.

# Features 
- table-table fk join
- stream-table join
- multiple topology application
- test utils for testing streams with avro
- [DONE] interactive query
- [todo] exception handling?
- [todo] use.latest.version
- [todo] windowed
- [todo] joinedwindow
- [todo] suppress window
- stream table join (the key schema, how is it set in two topics?)-optimization (.table and repartition)
- when stream-table join, how lookup works at byte level (how it works if there are two schema ids)
- when state lookup, how it works at byte level (two schema ids)
- tx producer 1 write to topic A, not yet committed, can tx producer 2 write to A and commit?


# Table-Table Join
- both tables have to be materialised for the topology to run
- See `TableTableForeignKeyJoinProcessor`

# Stream-Table Join
- See `StreamTableJoinProcessor`

# Multiple Topology Application
Demonstrates how a SpringBoot application can run multiple topologies. See `SecondaryTopologyConfiguration`

# Testing Streams with Avro
- See package `xiaoyf.demo.kafka.helper.serde`

# Interactive Query

# Error Handling
- todo

# `use.latest.version`
- todo

# Notes
## SUBJECT LIST for Table-Table FK Join
below are subjects created in schema registry related to the table-table fk join.
```
subject=customer-details-key, id=3, schema={"type":"record","name":"CustomerDetailsKey","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"}]}
subject=customer-details-value, id=4, schema={"type":"record","name":"CustomerDetails","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"},{"name":"name","type":{"type":"string","avro.java.string":"String"}},{"name":"email","type":{"type":"string","avro.java.string":"String"}},{"name":"activeCampaigns","type":{"type":"array","items":{"type":"string","avro.java.string":"String"}}}]}
subject=customer-order-key, id=1, schema={"type":"record","name":"CustomerOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"}]}
subject=customer-order-value, id=2, schema={"type":"record","name":"CustomerOrder","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"},{"name":"productName","type":{"type":"string","avro.java.string":"String"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"campaign","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}]}
subject=kafka-demo-CUST-DETAILS-TABLE-changelog-key, id=3, schema={"type":"record","name":"CustomerDetailsKey","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"}]}
subject=kafka-demo-CUST-DETAILS-TABLE-changelog-value, id=4, schema={"type":"record","name":"CustomerDetails","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"},{"name":"name","type":{"type":"string","avro.java.string":"String"}},{"name":"email","type":{"type":"string","avro.java.string":"String"}},{"name":"activeCampaigns","type":{"type":"array","items":{"type":"string","avro.java.string":"String"}}}]}
subject=kafka-demo-CUST-ORDER-TABLE-changelog-key, id=1, schema={"type":"record","name":"CustomerOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"}]}
subject=kafka-demo-CUST-ORDER-TABLE-changelog-value, id=2, schema={"type":"record","name":"CustomerOrder","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"},{"name":"productName","type":{"type":"string","avro.java.string":"String"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"campaign","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000005-topic-fk-key, id=3, schema={"type":"record","name":"CustomerDetailsKey","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000005-topic-key, id=3, schema={"type":"record","name":"CustomerDetailsKey","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000005-topic-pk-key, id=1, schema={"type":"record","name":"CustomerOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000005-topic-vh-value, id=2, schema={"type":"record","name":"CustomerOrder","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"},{"name":"productName","type":{"type":"string","avro.java.string":"String"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"campaign","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000013-topic-key, id=1, schema={"type":"record","name":"CustomerOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"}]}
subject=kafka-demo-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000013-topic-value, id=4, schema={"type":"record","name":"CustomerDetails","namespace":"demo.model","fields":[{"name":"customerNumber","type":"long"},{"name":"name","type":{"type":"string","avro.java.string":"String"}},{"name":"email","type":{"type":"string","avro.java.string":"String"}},{"name":"activeCampaigns","type":{"type":"array","items":{"type":"string","avro.java.string":"String"}}}]}
subject=kafka-demo-ORDER-JOINS-CUST-DETAILS-changelog-key, id=1, schema={"type":"record","name":"CustomerOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"}]}
subject=kafka-demo-ORDER-JOINS-CUST-DETAILS-changelog-value, id=5, schema={"type":"record","name":"PremiumOrder","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"},{"name":"productName","type":{"type":"string","avro.java.string":"String"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"campaign","type":{"type":"string","avro.java.string":"String"}},{"name":"name","type":{"type":"string","avro.java.string":"String"}},{"name":"email","type":{"type":"string","avro.java.string":"String"}}]}
subject=premium-order-key, id=6, schema={"type":"record","name":"PremiumOrderKey","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"}]}
subject=premium-order-value, id=5, schema={"type":"record","name":"PremiumOrder","namespace":"demo.model","fields":[{"name":"orderNumber","type":"long"},{"name":"customerNumber","type":"long"},{"name":"productName","type":{"type":"string","avro.java.string":"String"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"campaign","type":{"type":"string","avro.java.string":"String"}},{"name":"name","type":{"type":"string","avro.java.string":"String"}},{"name":"email","type":{"type":"string","avro.java.string":"String"}}]}
```