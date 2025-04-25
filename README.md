## What is a Distributed Database

top examples will be MongoDB Atlas 

### 1. Data Partitioning

core Idea : Parting the date into multiple nodes for scalability

Strategies Used :
  1. Hash Based : Each record is assigned to partition based on hash function applied to its primary key. For example `hash(key) % number_of_partitions`
  2. Range Based
  3. Composite Based : Combined hash-based and range-based Partitioning

