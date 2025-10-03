# Kafka Streams Abstractions

--------------------------------------------------------------------
## KStream

**Core Concept:**  
An abstraction representing an unbounded stream of records - an event log where each record is processed independently.

**Characteristics:**
- **Immutable records** - each record represents a fact/event that happened
- **Event semantics** - processes every single record, even duplicates for same key
- **Stateless by default** - no memory of previous records (unless explicitly added via stateful operations requiring [state store](#state-store))
- **Infinite stream** - continues processing as new records arrive

**Use Cases:**
- Real-time event processing (clicks, transactions, logs)
- ETL pipelines
- Event-driven microservices communication

--------------------------------------------------------------------
## KTable

**Core Concept:**  
An abstraction representing a changelog stream - models a database table where each record represents an update/upsert.

**Characteristics:**
- **Mutable state** - maintains current value for each key in a [state store](#state-store)
- **Database table semantics** - latest value wins for each key
- **Materialized view** - can be queried for current state
- **Changelog topic** - automatically creates internal topic to backup state store

**Key Behaviors:**
- **State store** - holds only the latest value per key
- **Fault tolerance** - changelog topic enables state recovery after failures
- **Compaction** - **underlying topic should be log-compacted** to retain latest values

**Use Cases:**
- User profiles/settings (latest state matters)
- Current inventory levels
- Real-time dashboards showing current metrics
- Reference data lookups

--------------------------------------------------------------------
## GlobalKTable

**Core Concept:**  
A fully replicated table where every application instance gets a complete copy of all data from all partitions.

**Characteristics:**
- **Full replication** - each instance has ALL data, not just local partitions
- **No co-partitioning required** - can join with any KStream/KTable regardless of partitioning
- **Read-only** - cannot be updated through stream operations
- **Eventually consistent** - updates propagate to all instances

**Key Differences from KTable:**
- **Partitioning**: KTable data is partitioned, GlobalKTable is fully replicated
- **Joins**: No [co-partitioning](#co-partitioning) requirements for joins
- **Resource usage**: Higher memory usage (full dataset per instance)
- **Scalability**: Limited by memory of single instance

**Use Cases:**
- Small reference data (countries, currencies, product catalogs)
- Configuration tables
- Lookup tables for enrichment
- Data that changes infrequently but needs fast access

--------------------------------------------------------------------
## Comparison Summary

| Aspect | KStream | KTable | GlobalKTable |
|--------|---------|---------|--------------|
| **Semantics** | Event log | Database table | Replicated table |
| **State** | Stateless (by default) | Stateful (state store) | Stateful (fully replicated) |
| **Processing** | Every record | Latest per key | Latest per key (all instances) |
| **Memory** | Low | Medium (local partitions) | High (full dataset) |
| **Joins** | Requires [co-partitioning](#co-partitioning) | Requires [co-partitioning](#co-partitioning) | No co-partitioning needed |
| **Use Case** | Events/facts | Current state | Small reference data |

--------------------------------------------------------------------

## Advanced Topics

### State Store

**Core Concept:**  
A local, persistent storage mechanism that maintains state for stateful operations in Kafka Streams.

**Characteristics:**
- **Local storage** - each application instance maintains its own state stores
- **Fault tolerance** - backed by changelog topics for recovery after failures
- **Key-value store** - typically stores data as key-value pairs
- **Partitioned** - each instance only stores state for its assigned partitions

**Types:**
- **In-memory** - fast access but lost on restart (unless changelog recovery)
- **RocksDB** - persistent storage on disk (default for most operations)
- **Custom** - can implement custom state store backends

**Use Cases:**
- KTable materialized views
- Aggregation results (count, sum, etc.)
- Join operations
- Custom processors that need to remember state

--------------------------------------------------------------------
### Co-partitioning

**Core Concept:**  
A guarantee that records with the same key, across multiple topics, are always placed in the partition with the same index.

**Requirements:**
- **Equal partition count** - all topics in the join must have the exact same number of partitions
- **Identical partitioning logic** - the same key-to-partition mapping strategy must be used for all topics

**When Required:**
- **KStream-KStream joins** - both streams must be co-partitioned
- **KStream-KTable joins** - both must be co-partitioned  
- **KTable-KTable joins** - both must be co-partitioned

**When NOT Required:**
- **KStream/KTable-GlobalKTable joins** - GlobalKTable is fully replicated, no co-partitioning needed

**Primary Goal:**  
Enable efficient joins by ensuring all data for a specific key is physically located on the same machine and processed by the same task.

**Automatic Repartitioning:**
If co-partitioning requirements are not met, Kafka Streams automatically creates internal repartition topics to satisfy the requirements.

**How to Ensure Co-partitioning:**

1. **At Kafka topic level (recommended)**
```bash
# Create topics with same partition count
kafka-topics --create --topic orders --partitions 4
kafka-topics --create --topic customers --partitions 4
```

2. **Producer with same partitioner**
```java
// Use same key and partitioning strategy
producer.send(new ProducerRecord<>("orders", customerId, order));
producer.send(new ProducerRecord<>("customers", customerId, customer));
```

3. **Kafka Streams automatic repartitioning**
```java
// If topics aren't co-partitioned, Streams automatically creates internal repartition topics
KStream<String, Order> orders = builder.stream("orders");
KTable<String, Customer> customers = builder.table("customers");

// Streams detects lack of co-partitioning and automatically repartitions
orders.join(customers, (order, customer) -> enrichedOrder);
```

4. **Explicit repartitioning**
```java
KStream<String, Order> repartitionedOrders = orders
    .selectKey((key, order) -> order.getCustomerId()) // key change
    .repartition(); // explicit repartitioning

repartitionedOrders.join(customers, joiner);
```

5. **GroupBy with repartitioning**
```java
KStream<String, Order> reKeyed = orders
    .groupBy((key, order) -> order.getCustomerId()) // key change causes repartitioning
    .aggregate(...)
    .toStream();
```

**Best Practices:**
- Plan partitioning at topic design stage
- Use the same business key for related data
- Let Kafka Streams automatically repartition when needed

--------------------------------------------------------------------