package com.streams.windows;

import com.streams.model.GameScoreEvent;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

/*
 * Tests TumblingWindowTopology - demonstrates Tumbling Time Windows with reduce()
 * 
 * Tumbling Windows:
 * - Fixed size, non-overlapping time windows
 * - Each record belongs to exactly one window
 * - Example: 60-second windows [0-60), [60-120), [120-180)...
 * 
 * Grace Period:
 * - Additional time after window end to accept late-arriving records
 * - Example: 20s grace means window [0-60) accepts records until 80s
 * - After grace period expires, window is closed and results are final
 */
public class TumblingWindowTopology {

    /**
     * Creates topology that aggregates game scores within 60-second tumbling windows.
     * 
     * WINDOW SEMANTICS:
     * - Window size: 60 seconds (tumbling, non-overlapping)
     * - Grace period: 20 seconds for late-arriving records
     * - Records are grouped by game name (stream key)
     * - Scores are summed within each window per game
     * 
     * LATE RECORDS HANDLING:
     * - Late record: has event time (record timestamp) from window but arrives after window closes
     * - Grace period: 20s additional time for late records after window closes
     * - Example: window [0-60s), grace 20s → record with event time 30s arriving at processing time 70s gets processed
     * - Records arriving after grace period (80s+) are dropped
     */
    public static Topology createTumblingWindowTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GameScoreEvent> gameScoreStream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(GameScoreEvent.class))
        );

        /*
         * Windowed aggregation with reduce():
         * 
         * 1. groupByKey() - groups by game name (key)
         * 2. windowedBy() - creates 60s tumbling windows with 20s grace
         * 3. reduce() - sums scores within each window
         * 
         * Result: KTable<Windowed<String>, GameScoreEvent>
         * - Key: Windowed<String> contains game name + window time range
         * - Value: GameScoreEvent with total score for that game in that window
         */
        KTable<Windowed<String>, GameScoreEvent> totalGameScoresInAMinute = gameScoreStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(20)))
                .reduce(
                        // Reducer: combines two scores from same game within same window
                        (gse1, gse2) -> {
                            return new GameScoreEvent(gse1.getGameName(), gse1.getScore() + gse2.getScore());
                        },
                        // Materialized: specifies WindowStore for persistent state
                        Materialized.<String, GameScoreEvent, WindowStore<Bytes, byte[]>>as("total-game-score-in-a-minute-state-store")
                            .withKeySerde(Serdes.String())
                            .withValueSerde(new JsonSerDes<>(GameScoreEvent.class))
                    );

        /*
         * Output windowed results:
         * 
         * 1. toStream() - converts KTable to KStream for output
         * 2. map() - flattens Windowed<String> key to String for easier consumption
         *    Format: "gameName@windowStartTimestamp"
         * 3. to() - sends results to output topic
         *
         */
        totalGameScoresInAMinute
                .toStream()
                .map((windowedKey, gameScoreEvent) -> new KeyValue<>(
                        windowedKey.key() + "@" + windowedKey.window().start(), 
                        gameScoreEvent
                ))
                .to(
                        "output-topic",
                        Produced.with(
                                Serdes.String(),
                                new JsonSerDes<>(GameScoreEvent.class)
                        )
                );

        return builder.build();
    }
}
