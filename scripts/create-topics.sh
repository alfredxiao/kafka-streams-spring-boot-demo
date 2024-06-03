#!/bin/bash

TOPICS="order
customer
preference
contact
long-id
order-deduped
order-enriched-by-global-store
order-enriched-by-global-ktable
order-enriched-by-regular-store
order-enriched-by-joining
customer-order-batch
long-id-to-short-id
short-id-to-long-id
preference-enriched
customer-enriched"

PARTITIONS=1
RF=1

echo "$TOPICS" | while IFS= read -r topic ;
do
  echo $topic
  kafka-topics --bootstrap-server localhost:9092 --create --topic $topic --partitions $PARTITIONS --replication-factor $RF
done

