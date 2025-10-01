# Kafka Streams Windows

## Kafka Record Structure

Every Kafka record has:
- `key`
- `value` (payload/message content)  
- `timestamp` (this field is used by Kafka Streams)
- `headers` (various metadata, but not timestamp)

## Time Types

### Event Time
When the event actually happened in real world.

Kafka Streams gets this timestamp from the `timestamp` field of Kafka record, not from message content. 

When producer sends a message to Kafka, it can include a timestamp.
If producer doesn't set timestamp, Kafka broker adds current time.

In rare edge cases, Kafka Streams may fall back to "stream time" - the highest timestamp it has seen so far in this partition.

What if you want to use timestamp from your payload (like "createdAt" field)? You need to implement TimestampExtractor to tell Kafka Streams where to find the timestamp in your data.

### Processing Time
When the stream processing application processes the record.

### Ingestion Time
When the record reached Kafka broker.  

## Tumbling Windows
A series of fixed-size windows which do not overlap.

Fixed-size, non-overlapping windows: [0-60), [60-120), [120-180)...

**Grace Period** - extra time to accept late records (but with event time within the window) after window closes

**Result Emission** - by default, Kafka Streams emits results as records arrive (controlled by commit.interval.ms, cache.max.bytes.buffering, linger.ms), not when window closes. To emit only after window closes, use `.suppress(Suppressed.untilWindowCloses())`

**Wall Clock Time** - real system time (like System.currentTimeMillis()). Used to determine when windows close, grace periods end, punctuations trigger. In tests, controlled by `testDriver.advanceWallClockTime()`

Example:
- Window [0-60s), grace 20s
- Record with event time 30s arriving at processing time 70s → accepted
- Record with event time 30s arriving after 80s → dropped