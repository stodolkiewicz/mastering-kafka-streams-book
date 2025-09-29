

### repartition
Creating new internal topic, so that data with the same key is in the same partition.

## Co-partitioning
**Core Concept:**  
A guarantee that records with the same key, across multiple topics,
are always placed in the partition with the same index (e.g., partition 0, 1, etc.).

**Primary Goal:**  
To enable efficient, stateful joins (KStream-KStream, KStream-KTable) by
ensuring all data for a specific key is physically located
on the same machine and processed by the same task.

**Strict Requirements:** 
- **Equal Partition Count**: All topics in the join must have the exact same number of partitions.
- **Identical Partitioning Logic**: The same key-to-partition mapping strategy must be used for all topics.