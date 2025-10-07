# Kafka Streams Joins - Complete Guide

## What are Joins in Kafka Streams?

A **join** is an operation that combines data from two different sources based on a common key.
In Kafka Streams, we have different types of joins depending on the data type (stream vs table).

---

## 1. Stream-Stream Joins (Windowed)

**What it is:** Combining two event streams within a specified time window.

**Why windowed?** Streams are infinite - without a time window, the join would wait indefinitely for the second event.

### Stream-Stream Join Types:

**Inner Join** - `.join()`
```
Stream A: Order(orderId=1, timestamp=10:00)
Stream B: Payment(orderId=1, timestamp=10:02)
Window: 5 minutes
Result: OrderPayment(orderId=1) // emitted at 10:02
```
- Emits result ONLY when BOTH events are present in the window

**Left Join** - `.leftJoin()`
```
Stream A: Order(orderId=1, timestamp=10:00)
Stream B: no Payment for orderId=1
Window: 5 minutes  
Result: OrderPayment(orderId=1, payment=null) // emitted at 10:05 (window close)
```
- Always emits when left stream event arrives (left stream is the "driver")
- Right stream can arrive first and be buffered, but only left stream arrival triggers output
- Right stream can be null

**Outer Join** - `.outerJoin()`
```
Stream A: Order(orderId=1, timestamp=10:00)
Stream B: Payment(orderId=2, timestamp=10:01) // different ID!
Window: 5 minutes
Result: 
- OrderPayment(order=Order1, payment=null) // at 10:05
- OrderPayment(order=null, payment=Payment2) // at 10:06
```
- Emits for every event from both streams

### Stream-Stream Requirements:
- **Common partitioning key** (orderId)
- **Time window** (e.g., 5 minutes)
- **Event timestamp** (for windowing)

### Windowing Behavior:
```java
// Symmetric window (default)
TimeWindows.of(Duration.ofMinutes(5))
// Order at 10:00 → window [10:00, 10:05]
// Payment can join between 10:00-10:05

// Asymmetric window (assumes orderStream.join(paymentStream, ...))
JoinWindows.of(Duration.ofMinutes(5))
    .before(Duration.ofMinutes(2))  // LEFT (Order) can be 2 min before RIGHT (Payment)
    .after(Duration.ofMinutes(3));  // RIGHT (Payment) can be 3 min after LEFT (Order)

// With grace period for late events
TimeWindows.of(Duration.ofMinutes(5))
    .grace(Duration.ofMinutes(1));  // Accept events 1 min late
```

**Important:** 
- Window is calculated based on **event timestamp**, not processing time
- JoinWindows direction is relative to join order: `leftStream.join(rightStream, ...)`

### Business Use Cases:
- **Order + Payment** - Real-time order fulfillment
- **User Login + Activity** - Session tracking
- **Click + Purchase** - Conversion funnel analysis

---

## 2. Stream-Table Joins

**What it is:** Enriching a stream with current state from a table.

**Example:**
```
Stream: UserActivity(userId=123, action="click")  
Table: UserProfile(userId=123, name="John", plan="Premium")
Result: EnrichedActivity(userId=123, action="click", name="John", plan="Premium")
```

### Characteristics:
- **No windowing** - table always has "current state"
- **Inner Join** - only if user exists in table
- **Left Join** - always emits, user can be null
- **Use case:** Adding context to events (profile, settings, permissions)

### Business Use Cases:
- **Event Enrichment** - Add user profile to activity events
- **Permission Checking** - Validate user access on each action
- **Personalization** - Customize content based on user preferences

---

## 3. Table-Table Joins

**What it is:** Combining two tables (current state).

**Example:**
```
Table A: UserProfile(userId=123, name="John")
Table B: UserSettings(userId=123, theme="dark") 
Result: UserWithSettings(userId=123, name="John", theme="dark")
```

### Characteristics:
- **Change propagation** - update in one table → new join result
- **Inner/Left/Outer Join** available
- **Foreign Key Joins** - joining on different keys
- **Use case:** Data denormalization, materialized views

### Business Use Cases:
- **User Denormalization** - Combine profile + settings + preferences
- **Product Catalog** - Join product info + inventory + pricing
- **Customer 360** - Aggregate all customer data

---

## 4. Global Table Joins

**What it is:** Joining a stream with a global table (replicated on every node).

### Advantages:
- **No repartitioning** - faster performance
- **Any join key** - doesn't need to be partitioning key
- **Eventual consistency**

**Example:**
```
Stream: Transaction(userId=123, currency="USD", amount=100)
Global Table: ExchangeRates(currency="USD", rate=1.2)
Join Key: transaction.currency = table.currency (not userId!)
Result: TransactionInEUR(userId=123, amountEUR=120)
```

### Limitations:
- **Read-only** - cannot modify global table through join
- **Memory usage** - entire table in memory on each node

### Business Use Cases:
- **Currency Conversion** - Real-time rate application
- **Reference Data** - Country codes, tax rates, product categories
- **Configuration** - Apply settings without repartitioning

---

## When to Use Which Join?

| Join Type | When to Use | Example | Windowing |
|-----------|-------------|---------|-----------|
| **Stream-Stream** | Real-time event correlation | Order + Payment | Required |
| **Stream-Table** | Event enrichment | User activity + profile | No |
| **Table-Table** | Data denormalization | User profile + settings | No |
| **Global Table** | Reference data lookup | Currency rates, codes | No |

---

## Window Types in Kafka Streams

Understanding window types is crucial for both joins and aggregations. Kafka Streams provides different windowing mechanisms for different use cases.

### TimeWindows vs JoinWindows - Visual Comparison

```
TimeWindows (Fixed Buckets):
Timeline: ----[10:00----10:05)----[10:05----10:10)----[10:10----10:15)----
          
Event A (10:02) → │ Bucket 1 │
Event B (10:07) → │ Bucket 2 │
Event C (10:12) → │ Bucket 3 │

Each event goes to exactly ONE bucket based on timestamp.
```

```
JoinWindows (Sliding Windows):
Timeline: ----10:00----10:05----10:10----10:15----

Order at 10:05:  [----10:00────10:05────10:10----]  (5-min window)
                        ↑       ↑       ↑
                   Payment   Order   Payment
                   (10:03)  (10:05)  (10:08)
                     ✓       ✓        ✓
                   MATCH    SELF     MATCH

Order can match with MULTIPLE payments within its window.
```

### Fundamental Differences

| Aspect | TimeWindows | JoinWindows |
|--------|-------------|-------------|
| **Purpose** | Grouping events into time buckets | Defining matching criteria for joins |
| **Behavior** | Fixed time intervals (buckets) | Sliding window around each event |
| **Usage** | `groupBy().windowedBy(TimeWindows...)` | `stream.join(stream, joiner, JoinWindows...)` |
| **Event Assignment** | One bucket per event | Multiple potential matches per event |
| **Symmetry** | Always symmetric | Can be asymmetric (.before/.after) |
| **Memory Pattern** | Stores aggregate per bucket | Buffers raw events for matching |

### TimeWindows - For Aggregations

**Concept:** Divides the timeline into fixed, non-overlapping time buckets.

```java
// Fixed 5-minute windows
TimeWindows.of(Duration.ofMinutes(5))
// Creates buckets: [10:00-10:05), [10:05-10:10), [10:10-10:15), ...

// With grace period for late events
TimeWindows.of(Duration.ofMinutes(5))
    .grace(Duration.ofMinutes(1)); // Accept events up to 1 minute late

// Without grace period (immediate window closing)
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));

// Advanced creation with explicit grace
TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30));
```

**Behavior Example:**
```
Sales Aggregation with 1-hour TimeWindows:

Timeline: [09:00-10:00) [10:00-11:00) [11:00-12:00)
          │    $500    │   $1200     │    $800    │
          
Sale at 09:30 ($100) → Window [09:00-10:00) → Total: $500
Sale at 10:15 ($300) → Window [10:00-11:00) → Total: $1200  
Sale at 11:45 ($200) → Window [11:00-12:00) → Total: $800

Each sale contributes to exactly ONE window's total.
```

**Use Cases:**
- **Hourly/daily sales reports** - Fixed time intervals
- **IoT sensor readings aggregation** - Regular time buckets
- **Click-through rate calculation** - Windowed counters
- **Real-time dashboards** - Time-based metrics

### JoinWindows - For Stream-Stream Joins

**Concept:** Creates a sliding window around each event to find matching events from another stream.

```java
// Symmetric 5-minute join window
JoinWindows.of(Duration.ofMinutes(5))
// Event can match with events 5 min before OR after

// Asymmetric window
JoinWindows.of(Duration.ofMinutes(10))
    .before(Duration.ofMinutes(2))  // 2 min before
    .after(Duration.ofMinutes(8));  // 8 min after

// Modern API with explicit time difference
JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
    .before(Duration.ofMinutes(0)); // No events before allowed

// With grace period
JoinWindows.ofTimeDifferenceAndGrace(
    Duration.ofMinutes(5), 
    Duration.ofMinutes(1)
);
```

**Symmetric Window Behavior:**
```
Order-Payment Join with 10-minute symmetric window:

Timeline: ----10:00----10:05----10:10----10:15----10:20----

Order at 10:10:    [10:00──────10:10──────10:20]
                     ↑          ↑          ↑
               Payment 1    Order     Payment 2
               (10:02)     (10:10)    (10:18)
                 ✓          ✓          ✓
               MATCH       SELF       MATCH

Order can join with Payment 1 (8 min before) and Payment 2 (8 min after).
```

**Asymmetric Window Behavior:**
```
Order-Payment Join with asymmetric window (.before(0), .after(15)):

Timeline: ----10:00----10:05----10:10----10:15----10:20----10:25----

Order at 10:10:         [10:10────────────────────10:25]
                          ↑                         ↑
                       Order                   Payment
                      (10:10)                  (10:22)
                         ✓                       ✓
                       SELF                    MATCH

Payment at 10:05 → NO MATCH (before Order, but .before(0))
Payment at 10:22 → MATCH (12 min after Order, within .after(15))
```

### SessionWindows - Gap-Based Grouping

```java
SessionWindows.with(Duration.ofMinutes(5))
// Groups events separated by less than 5 minutes into same session
```

**Behavior Example:**
```
User Activity Sessions (5-minute inactivity gap):

Timeline: ----10:00----10:05----10:10----10:15----10:20----10:25----

Activity:      A        A               A        A        A
              ↓        ↓               ↓        ↓        ↓
           10:01    10:03           10:12    10:14    10:16

Sessions: [─Session 1─]  5min gap  [────Session 2────]
          (10:01-10:03)             (10:12-10:16)

Gap > 5 minutes → New session starts
```

**Use Cases:**
- **User session tracking** - Web/mobile app sessions
- **Batch job detection** - Group related processing events
- **Conversation threads** - Chat/email message grouping

### SlidingWindows - Overlapping Time Windows

```java
SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
// Every event creates a 5-minute window, windows overlap
```

**Behavior Example:**
```
Moving Average with 5-minute SlidingWindows:

Timeline: ----10:00----10:02----10:04----10:06----10:08----

Event A (10:00): [10:00────10:05]
Event B (10:02):    [10:02────10:07]  ← Overlaps with A
Event C (10:04):       [10:04────10:09]  ← Overlaps with A & B
Event D (10:06):          [10:06────10:11]  ← Overlaps with B & C

Each event participates in multiple overlapping windows.
```

**Use Cases:**
- **Moving averages** - Smooth metric transitions
- **Real-time trend analysis** - Overlapping time periods
- **Anomaly detection** - Continuous monitoring windows

### Grace Periods and Late Events

**Grace Period** allows processing of late-arriving events:

```
Window Lifecycle with Grace Period:

Timeline: ----10:00────10:05────10:06────10:07────

Window [10:00-10:05):
                ↑         ↑         ↑
           Window      Grace     Final
           Closes      Period    Close
           (10:05)     (10:06)   (10:07)
           
Events arriving between 10:05-10:06 still processed!
Events arriving after 10:07 are dropped.
```

```java
// Configure grace period
TimeWindows.of(Duration.ofMinutes(5))
    .grace(Duration.ofMinutes(1));  // 1-minute grace

// Trade-offs:
// Longer grace = Better late event handling + Higher memory usage
// No grace = Lower latency + Risk of missing late events
```

### Memory and Performance Implications

#### TimeWindows Memory Pattern
```
Aggregation State Storage:

Key: customer-123
Windows: [10:00-11:00) → {count: 5, sum: 250.0}
         [11:00-12:00) → {count: 3, sum: 180.0}
         [12:00-13:00) → {count: 7, sum: 420.0}

Memory per key = Number of active windows × Aggregate size
```

#### JoinWindows Memory Pattern  
```
Event Buffering for Joins:

Left Stream Buffer (Orders):
  order-123 → [Order(10:05), Order(10:08)]
  
Right Stream Buffer (Payments):  
  order-123 → [Payment(10:07), Payment(10:09)]

Memory per key = Window size × Event rate × Event size
```

**Performance Guidelines:**

```java
// ❌ Memory-intensive
JoinWindows.of(Duration.ofHours(24))  // 24-hour buffer!

// ✅ Memory-efficient  
JoinWindows.of(Duration.ofMinutes(5))
    .before(Duration.ZERO)  // Reduce buffering

// ❌ Too many small windows
TimeWindows.of(Duration.ofSeconds(1))  // 3600 windows/hour

// ✅ Reasonable window size
TimeWindows.of(Duration.ofMinutes(5))  // 12 windows/hour
```

### When to Use Which Window Type

| Scenario | Window Type | Rationale |
|----------|-------------|-----------|
| **Hourly sales reports** | `TimeWindows.of(Duration.ofHours(1))` | Fixed reporting intervals |
| **Order-Payment matching** | `JoinWindows.of(Duration.ofMinutes(10))` | Event correlation tolerance |
| **User session analysis** | `SessionWindows.with(Duration.ofMinutes(30))` | Activity-gap based grouping |
| **Real-time moving average** | `SlidingWindows.of(Duration.ofMinutes(5))` | Smooth metric transitions |
| **Fraud detection (ordered events)** | `JoinWindows.of(...).before(Duration.ZERO)` | Enforce temporal ordering |
| **IoT sensor aggregation** | `TimeWindows.of(...).grace(Duration.ofMinutes(2))` | Handle network delays |
| **Global daily totals** | `UnlimitedWindows.of()` | No time constraints |

### Common Anti-Patterns and Solutions

❌ **Mixing window types incorrectly:**
```java
// WRONG - TimeWindows for joins
stream.join(otherStream, joiner, TimeWindows.of(...))  // Won't compile

// WRONG - JoinWindows for aggregations  
stream.groupByKey()
     .windowedBy(JoinWindows.of(...))  // Won't compile
```

✅ **Correct usage:**
```java
// Aggregation
stream.groupByKey()
     .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
     .count()

// Join
leftStream.join(rightStream, joiner, 
               JoinWindows.of(Duration.ofMinutes(5)))
```

❌ **Oversized windows causing memory issues:**
```java
// WRONG - Will consume excessive memory
JoinWindows.of(Duration.ofDays(1))
TimeWindows.of(Duration.ofSeconds(1)).grace(Duration.ofHours(1))
```

✅ **Right-sized windows:**
```java
// GOOD - Reasonable window sizes
JoinWindows.of(Duration.ofMinutes(10))
TimeWindows.of(Duration.ofMinutes(5)).grace(Duration.ofMinutes(1))
```

Understanding these window types and their behavior is essential for designing efficient, correct Kafka Streams applications that handle time-based operations properly while managing memory usage effectively.

---

## Key Concepts

### Co-partitioning
Stream-Stream and Stream-Table joins require co-partitioning:
```
// Both topics must have:
// 1. Same partitioning key (orderId)
// 2. Same number of partitions
// 3. Same partitioning strategy (hash function)

KStream<String, Order> orders = ...;      // Topic: orders (10 partitions)
KStream<String, Payment> payments = ...;  // Topic: payments (10 partitions)

// Without co-partitioning, Kafka Streams will automatically repartition
// (expensive operation with intermediate topics)
```

### Time Semantics
- **Event time** - timestamp in the event data
- **Processing time** - time when event is processed
- **Window grace period** - additional time for late-arriving events

### State Stores
Joins use local state stores to buffer data:
- **RocksDB** - persistent local storage
- **In-memory** - faster but non-persistent
- **Retention time** - how long to keep join state

### Join Value Combiner
Custom logic for combining joined values:
```java
// Example: Order + Payment → OrderWithPayment
ValueJoiner<Order, Payment, OrderWithPayment> joiner = 
    (order, payment) -> new OrderWithPayment(order, payment);
```

---

## Performance Considerations

1. **Window Size** - Larger windows = more memory usage
2. **Grace Period** - Balance between late events and resource usage
3. **State Store Tuning** - Configure cache and commit intervals
4. **Partitioning Strategy** - Ensure even data distribution

---

## Common Patterns

### Error Handling in Joins
```java
// Handle null values in join result
ValueJoiner<Order, Payment, OrderResult> joiner = (order, payment) -> {
    if (payment == null) {
        return new OrderResult(order, "UNPAID");
    }
    if (payment.getTimestamp().isBefore(order.getTimestamp())) {
        return new OrderResult(order, "FRAUD_SUSPECTED");
    }
    return new OrderResult(order, payment, "PAID");
};
```

### Time-based Business Logic
```java
// Only join if payment comes after order
ValueJoiner<Order, Payment, OrderPayment> timeAwareJoiner = 
    (order, payment) -> {
        if (payment != null && 
            payment.getTimestamp().isAfter(order.getTimestamp())) {
            return new OrderPayment(order, payment);
        }
        return null; // Skip invalid joins
    };
```

---

## Enforcing Event Ordering

In many business scenarios, you need to ensure that events occur in a specific order (e.g., Order must come before Payment). Here are strategies to enforce this:

### Strategy 1: Custom ValueJoiner with Timestamp Validation
```java
ValueJoiner<Order, Payment, OrderResult> orderingJoiner = (order, payment) -> {
    if (payment != null && 
        payment.getTimestamp().isBefore(order.getTimestamp())) {
        // Payment before Order = suspicious activity
        return new OrderResult(order, payment, "FRAUD_SUSPECTED");
    }
    if (payment != null) {
        return new OrderResult(order, payment, "VALID_PAYMENT");
    }
    return new OrderResult(order, null, "PENDING_PAYMENT");
};
```

**Pros:**
- Simple to implement
- Flexible business logic
- Can handle edge cases (fraud detection)

**Cons:**
- Still processes "invalid" joins
- Relies on accurate event timestamps

### Strategy 2: Asymmetric JoinWindows
```java
// Prevent Payment from being before Order
JoinWindows.of(Duration.ofMinutes(10))
    .before(Duration.ZERO)           // Payment cannot be before Order
    .after(Duration.ofMinutes(10));  // Payment can be up to 10 min after Order

// Usage
orderStream.join(paymentStream, joiner, asymmetricWindow)
```

**Pros:**
- Framework-level enforcement
- Prevents invalid joins entirely
- Better performance (fewer join attempts)

**Cons:**
- Less flexible than custom logic
- Harder to debug when events are filtered out

### Strategy 3: Left Join with Order as Primary
```java
// Only Order can initiate the join
orderStream.leftJoin(paymentStream, joiner, windows)
    .filter((key, result) -> result.isValid());
```

**Pros:**
- Order-driven processing
- Can detect abandoned orders (no payment)

**Cons:**
- Still allows payment-before-order within window
- Need additional filtering logic

### Strategy 4: Custom Transformer with State Store
```java
public class OrderPaymentTransformer implements Transformer<String, Order, KeyValue<String, OrderResult>> {
    private KeyValueStore<String, Order> pendingOrders;
    private KeyValueStore<String, Set<Payment>> orphanedPayments;
    
    @Override
    public void init(ProcessorContext context) {
        this.pendingOrders = context.getStateStore("pending-orders");
        this.orphanedPayments = context.getStateStore("orphaned-payments");
    }
    
    @Override
    public KeyValue<String, OrderResult> transform(String orderId, Order order) {
        // Store the order
        pendingOrders.put(orderId, order);
        
        // Check for existing payments
        Set<Payment> payments = orphanedPayments.get(orderId);
        if (payments != null) {
            // Find valid payments (after order timestamp)
            Payment validPayment = payments.stream()
                .filter(p -> p.getTimestamp().isAfter(order.getTimestamp()))
                .findFirst()
                .orElse(null);
                
            if (validPayment != null) {
                // Clean up
                orphanedPayments.delete(orderId);
                pendingOrders.delete(orderId);
                return KeyValue.pair(orderId, new OrderResult(order, validPayment, "MATCHED"));
            }
        }
        
        // Schedule punctuator to check for abandoned orders
        return null; // Will emit when payment arrives
    }
}

// Separate transformer for payments
public class PaymentTransformer implements Transformer<String, Payment, KeyValue<String, OrderResult>> {
    // Similar logic but checks for existing orders first
    // If no order exists, store as orphaned payment
    // If order exists and payment timestamp > order timestamp, emit result
}
```

**Pros:**
- Complete control over ordering logic
- Can handle complex business rules
- State persistence across restarts
- Can implement timeouts and cleanup

**Cons:**
- Most complex to implement
- Requires careful state management
- Higher memory usage
- Need to handle punctuators for cleanup

### Strategy 5: Preprocessing with Stream Ordering
```java
// Pre-sort events by timestamp before joining
KStream<String, Event> allEvents = orderStream
    .map((k, order) -> KeyValue.pair(k, new Event("ORDER", order.getTimestamp(), order)))
    .merge(paymentStream
        .map((k, payment) -> KeyValue.pair(k, new Event("PAYMENT", payment.getTimestamp(), payment))));

// Group by key and process in timestamp order
allEvents
    .groupByKey()
    .aggregate(
        () -> new OrderPaymentAggregator(),
        (key, event, aggregator) -> aggregator.process(event),
        // Only emit when both order and payment are received in correct order
    );
```

**Pros:**
- Guarantees processing order
- Can handle complex multi-event workflows

**Cons:**
- Requires buffering and sorting
- Higher latency
- More complex state management

### Recommendation

**For simple use cases:** Use **Strategy 2 (Asymmetric JoinWindows)** - it's clean and enforced by the framework.

**For business logic validation:** Use **Strategy 1 (Custom ValueJoiner)** - allows fraud detection and flexible rules.

**For complex workflows:** Consider **Strategy 4 (Custom Transformer)** - provides maximum control but requires more development effort.


This documentation covers all major join types in Kafka Streams. Each join type serves different business needs and has specific requirements and characteristics.