package com.streams.windows;

import com.streams.common.model.GameScoreEvent;
import com.streams.common.serde.JsonDeserializer;
import com.streams.common.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TumblingWindowWithSuppressTopologyTest {
    TopologyTestDriver testDriver;
    TestInputTopic<String, GameScoreEvent> inputTopic;
    TestOutputTopic<String, GameScoreEvent> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        // Unique APPLICATION_ID per test to avoid state pollution
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppressWindowApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");

        Topology topology = TumblingWindowWithSupressTopology.createSuppressedTumblingWindowTopology();

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(GameScoreEvent.class)
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testSuppressedWindow_noEmissionDuringWindow() {
        // Send record with event time 30s (within window [0-60s))
        GameScoreEvent event = new GameScoreEvent("Mario", 100);
        inputTopic.pipeInput("Mario", event, Instant.ofEpochSecond(30));

        // Check that nothing is emitted while window is still open
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        assertTrue(results.isEmpty(), "No results should be emitted before window closes");
    }

    @Test
    void testSuppressedWindow_emissionAfterWindowCloses() {
        // Send first record with event time 30s
        GameScoreEvent event1 = new GameScoreEvent("Mario", 100);
        inputTopic.pipeInput("Mario", event1, Instant.ofEpochSecond(30));

        // Check nothing emitted yet
        Map<String, GameScoreEvent> resultsBeforeClose = outputTopic.readKeyValuesToMap();
        assertTrue(resultsBeforeClose.isEmpty(), "No results before window closes");

        // Send record with event time 85s to advance stream time beyond grace period
        // This should close the window [0-60s) and emit suppressed results
        GameScoreEvent advanceEvent = new GameScoreEvent("Other", 1);
        inputTopic.pipeInput("Other", advanceEvent, Instant.ofEpochSecond(85));

        // Now check that suppressed results are emitted
        Map<String, GameScoreEvent> resultsAfterClose = outputTopic.readKeyValuesToMap();
        assertFalse(resultsAfterClose.isEmpty(), "Results should be emitted after window closes");
        assertEquals(100, resultsAfterClose.get("Mario@0").getScore());
    }
}