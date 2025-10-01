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
 * TumblingWindowWithSuppressTopology - demonstrates Suppress behavior in windowed operations
 * 
 * Key Focus: SUPPRESSION
 * - Shows difference between immediate emission vs suppressed emission
 * - Results emitted only after window closes (after grace period)
 * - Useful for final results without intermediate updates
 * 
 * Suppression Benefits:
 * - Reduces downstream processing load
 * - Provides complete window results only
 * - Eliminates intermediate partial aggregations
 * 
 * See Also:
 * - {@link TumblingWindowTopology} for basic tumbling window concepts
 * - windows.md for detailed suppression documentation
 */
public class TumblingWindowWithSupressTopology {

    /**
     * Creates topology demonstrating SUPPRESSION in windowed aggregations.
     * 
     * SUPPRESSION FOCUS:
     * - Results NOT emitted immediately when records arrive
     * - Results emitted ONLY after window closes (post grace period)
     * - Demonstrates buffering behavior with suppress()
     * 
     * COMPARISON:
     * - Without suppress: immediate emission on each record
     * - With suppress: single emission after window closes
     * - Use case: final aggregated results only
     * 
     * @see TumblingWindowTopology#createTumblingWindowTopology() for basic windowing
     * 
     * For suppression configuration options see: windows.md#suppression
     */
    public static Topology createSuppressedTumblingWindowTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GameScoreEvent> gameScoreStream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(GameScoreEvent.class))
        );

        /*
         * Windowed aggregation with SUPPRESSION:
         * 
         * 1. groupByKey() - groups by game name (key)
         * 2. windowedBy() - creates 60s tumbling windows with 20s grace
         * 3. reduce() - sums scores within each window
         * 4. suppress() - KEY FEATURE: delays emission until window closes
         * 
         * Suppression Effect:
         * - Buffers intermediate results in memory
         * - Emits only final aggregated result per window
         * - No partial updates during window lifetime
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
                    )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        /*
         * Output suppressed windowed results:
         * 
         * 1. suppress() - delays emission until window closes (after grace period)
         * 2. toStream() - converts KTable to KStream for output
         * 3. map() - flattens Windowed<String> key to String for easier consumption
         *    Format: "gameName@windowStartTimestamp"
         * 4. to() - sends results to output topic
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
