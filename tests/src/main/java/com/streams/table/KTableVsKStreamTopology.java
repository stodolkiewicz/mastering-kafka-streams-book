package com.streams.table;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;


/*
 * KStream vs KTable:
 * 
 * KStream:
 * - No state store (stateless by default)
 * - No changelog topic
 * - Can be stateful through operations like groupBy().aggregate() (creates state store)
 * 
 * KTable:
 * - Always has state store (holds current state per key)
 * - Always has changelog topic (backup of state store)
 * - Update semantics: new value overwrites old value for same key
 */
public class KTableVsKStreamTopology {
/*
      ┌─────────────────┐
      │   input-topic   │
      └─────────┬───────┘
                │
                ▼
      ┌─────────────────┐
      │    KStream      │
      └─────┬───────┬───┘
            │       │
            │       │ toTable
            │       ▼
            │   ┌─────────────────┐
            │   │     KTable      │
            │   └─────────┬───────┘
            │             │ (toStream + to)
            ▼             ▼
    ┌─────────────────┐ ┌─────────────────┐
    │stream-output-   │ │k-table-output-  │
    │     topic       │ │     topic       │
    └─────────────────┘ └─────────────────┘

    */
    public static Topology createKStreamVsKTableTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Input stream
        KStream<String, Long> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), Serdes.Long())
        );

        // Emit every record unchanged to stream-output-topic
        stream.to(
                "stream-output-topic",
                Produced.with(
                        Serdes.String(),
                        Serdes.Long()
                )
        );

        // convert stream to table and emit to k-table-output-topic
        KTable<String, Long> kTable = stream.toTable(
                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("kTable-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
        );
        kTable.toStream().to(
                "k-table-output-topic",
                Produced.with(
                        Serdes.String(),
                        Serdes.Long()
                ));

        return builder.build();
    }
}
