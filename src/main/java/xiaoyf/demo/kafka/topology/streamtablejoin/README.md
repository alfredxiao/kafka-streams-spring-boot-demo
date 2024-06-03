demonstrates how schema id is used 


steps
1. generates contact with key v1 (results in v1 contact in contact-table)
2. modify key definition with slight change
3. generates customer with key v2 (results in failed lookup - even if partition count is 1 where all records routed to same partition)
4. 