package com.streams.windows;

import com.streams.model.GameScoreEvent;
import com.streams.model.MyModel;
import com.streams.serde.JsonDeserializer;
import com.streams.serde.JsonSerDes;
import com.streams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TumblingWindowTopologyTest {
    TopologyTestDriver testDriver;
    TestInputTopic inputTopic;
    TestOutputTopic outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        // Unique APPLICATION_ID per test to avoid state pollution
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumblingWindowApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");

        Topology topology = TumblingWindowTopology.createTumblingWindowTopology();

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
    void testTumblingWindow_recordsInSameWindowAreReduced() {
        // given
        GameScoreEvent marioGSE1 = new GameScoreEvent("Mario", 1250);
        GameScoreEvent marioGSE2 = new GameScoreEvent("Mario", 500);
        GameScoreEvent marioGSE3 = new GameScoreEvent("Mario", 100);

        // when
            // first window
            inputTopic.pipeInput("Mario", marioGSE1, Instant.ofEpochSecond(0));
            inputTopic.pipeInput("Mario", marioGSE2, Instant.ofEpochSecond(55));

            // next minute - second window
            inputTopic.pipeInput("Mario", marioGSE3, Instant.ofEpochSecond(65));


        // then
        Map<String, GameScoreEvent> totalGameScoresInAMinute = outputTopic.readKeyValuesToMap();
        GameScoreEvent totalGsInAMinuteFirstWindow = totalGameScoresInAMinute.get("Mario@0");
        GameScoreEvent totalGsInAMinuteSecondWindow = totalGameScoresInAMinute.get("Mario@60000");

        assertEquals(1750, totalGsInAMinuteFirstWindow.getScore());
        assertEquals(100, totalGsInAMinuteSecondWindow.getScore());
    }

    @Test
    void testTumblingWindow_windowWithoutEvents() {
        // given
        GameScoreEvent marioGSE1 = new GameScoreEvent("Mario", 1250);
        GameScoreEvent marioGSE2 = new GameScoreEvent("Mario", 500);
        GameScoreEvent marioGSE3 = new GameScoreEvent("Mario", 100);

        // when
        // first window
        inputTopic.pipeInput("Mario", marioGSE1, Instant.ofEpochSecond(0));
        inputTopic.pipeInput("Mario", marioGSE2, Instant.ofEpochSecond(55));

        // nothing in second minute. second window in third minute
        inputTopic.pipeInput("Mario", marioGSE3, Instant.ofEpochSecond(125));


        // then
            //test output topic
                Map<String, GameScoreEvent> totalGameScoresInAMinute = outputTopic.readKeyValuesToMap();
                GameScoreEvent totalGsInAMinuteFirstWindow = totalGameScoresInAMinute.get("Mario@0");
                GameScoreEvent totalGsInAMinuteSecondWindow = totalGameScoresInAMinute.get("Mario@120000");

                assertEquals(1750, totalGsInAMinuteFirstWindow.getScore());
                assertEquals(100, totalGsInAMinuteSecondWindow.getScore());

            // test window store content
                WindowStore<String, GameScoreEvent> windowStore = testDriver.getWindowStore("total-game-score-in-a-minute-state-store");

                WindowStoreIterator<GameScoreEvent> iterator1 = windowStore.fetch("Mario", 0L, 60000L-1L);
                WindowStoreIterator<GameScoreEvent> iterator2 = windowStore.fetch("Mario", 60000L, 120000L-1L);
                WindowStoreIterator<GameScoreEvent> iterator3 = windowStore.fetch("Mario", 120000L, 180000L-1L);

                assertEquals(1750, iterator1.next().value.getScore());
                assertFalse(iterator2.hasNext());
                assertEquals(100, iterator3.next().value.getScore());

                iterator1.close();
                iterator2.close();
                iterator3.close();
    }

    @Test
    void testTumblingWindow_multipleKeysInSameWindow() {
        // given
        GameScoreEvent marioGSE = new GameScoreEvent("Mario", 1000);
        GameScoreEvent luigiGSE = new GameScoreEvent("Luigi", 500);

        // when - both in same window [0-60s)
        inputTopic.pipeInput("Mario", marioGSE, Instant.ofEpochSecond(30));
        inputTopic.pipeInput("Carts-2", luigiGSE, Instant.ofEpochSecond(45));

        // then
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        
        assertEquals(1000, results.get("Mario@0").getScore());
        assertEquals(500, results.get("Carts-2@0").getScore());
        
        // Verify separate aggregation per key in window store
        WindowStore<String, GameScoreEvent> windowStore = testDriver.getWindowStore("total-game-score-in-a-minute-state-store");
        
        WindowStoreIterator<GameScoreEvent> marioIterator = windowStore.fetch("Mario", 0L, 60000L-1L);
        WindowStoreIterator<GameScoreEvent> luigiIterator = windowStore.fetch("Carts-2", 0L, 60000L-1L);
        
        assertEquals(1000, marioIterator.next().value.getScore());
        assertEquals(500, luigiIterator.next().value.getScore());
        
        marioIterator.close();
        luigiIterator.close();
    }

    @Test
    void testTumblingWindow_outOfOrderRecords() {
        // given
        GameScoreEvent event1 = new GameScoreEvent("Mario", 100);
        GameScoreEvent event2 = new GameScoreEvent("Mario", 200);
        GameScoreEvent event3 = new GameScoreEvent("Mario", 300);

        // when - send records out of chronological order
        inputTopic.pipeInput("Mario", event1, Instant.ofEpochSecond(50)); // 50s
        inputTopic.pipeInput("Mario", event2, Instant.ofEpochSecond(10)); // 10s (earlier!)
        inputTopic.pipeInput("Mario", event3, Instant.ofEpochSecond(30)); // 30s

        // then - all should be aggregated in same window [0-60s)
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        assertEquals(600, results.get("Mario@0").getScore()); // 100 + 200 + 300

        // Verify in window store
        WindowStore<String, GameScoreEvent> windowStore = testDriver.getWindowStore("total-game-score-in-a-minute-state-store");
        WindowStoreIterator<GameScoreEvent> iterator = windowStore.fetch("Mario", 0L, 60000L-1L);
        
        assertEquals(600, iterator.next().value.getScore());
        iterator.close();
    }

    @Test
    void testTumblingWindow_lateEventsInGracePeriod() {
        // given
        GameScoreEvent event1 = new GameScoreEvent("Mario", 100);
        GameScoreEvent event2 = new GameScoreEvent("Mario", 200);
        GameScoreEvent event3 = new GameScoreEvent("Mario", 300);

        // when - send records out of chronological order
        inputTopic.pipeInput("Mario", event1, Instant.ofEpochSecond(50)); // 50s
        inputTopic.pipeInput("Mario", event2, Instant.ofEpochSecond(10)); // 10s (earlier!)
        inputTopic.pipeInput("Mario", event3, Instant.ofEpochSecond(30)); // 30s

        // then - all should be aggregated in same window [0-60s)
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        assertEquals(600, results.get("Mario@0").getScore()); // 100 + 200 + 300

        // Verify in window store
        WindowStore<String, GameScoreEvent> windowStore = testDriver.getWindowStore("total-game-score-in-a-minute-state-store");
        WindowStoreIterator<GameScoreEvent> iterator = windowStore.fetch("Mario", 0L, 60000L-1L);

        assertEquals(600, iterator.next().value.getScore());
        iterator.close();
    }

    @Test
    void testTumblingWindow_lateRecordsWithinGracePeriod() {
        // First record with event time 30s in window [0-60s)
        GameScoreEvent event1 = new GameScoreEvent("Mario", 100);
        inputTopic.pipeInput("Mario", event1, Instant.ofEpochSecond(30));

        // Advance stream time past window end (60s) but within grace period
        GameScoreEvent advanceEvent = new GameScoreEvent("Other", 1);
        inputTopic.pipeInput("Other", advanceEvent, Instant.ofEpochSecond(70));

        // Late record with event time from closed window [0-60s)
        // Stream time = 70s, grace period ends at 80s, so still within grace
        GameScoreEvent lateEvent = new GameScoreEvent("Mario", 50);
        inputTopic.pipeInput("Mario", lateEvent, Instant.ofEpochSecond(40));

        // Late record should be accepted: 100 + 50 = 150
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        assertEquals(150, results.get("Mario@0").getScore());
    }

    @Test
    void testTumblingWindow_lateRecordsOutsideOfGracePeriod() {
        // First record with event time 30s
        GameScoreEvent event1 = new GameScoreEvent("Mario", 100);
        inputTopic.pipeInput("Mario", event1, Instant.ofEpochSecond(30));

        // Advance stream time beyond grace period with dummy record
        // Grace period ends at 80s (60s window end + 20s grace)
        GameScoreEvent dummyEvent = new GameScoreEvent("Dummy", 1);
        inputTopic.pipeInput("Dummy", dummyEvent, Instant.ofEpochSecond(85));

        // Late record with event time 40s arrives after grace period expired
        GameScoreEvent lateEvent = new GameScoreEvent("Mario", 50);
        inputTopic.pipeInput("Mario", lateEvent, Instant.ofEpochSecond(40));

        // Late record should be dropped - only first record should remain
        Map<String, GameScoreEvent> results = outputTopic.readKeyValuesToMap();
        assertEquals(100, results.get("Mario@0").getScore());
        assertNull(results.get("Mario@60000"));
    }
}


