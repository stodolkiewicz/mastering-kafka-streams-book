package com.streams.table;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class KTableVsKStreamTopologyTest {
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Long> streamOutputTopic;
    private TestOutputTopic<String, Long> ktableOutputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testKTableVsKStreamApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = KTableVsKStreamTopology.createKStreamVsKTableTopology();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                Serdes.Long().serializer()
        );

        streamOutputTopic = testDriver.createOutputTopic(
                "stream-output-topic",
                Serdes.String().deserializer(),
                Serdes.Long().deserializer()
        );

        ktableOutputTopic = testDriver.createOutputTopic(
                "k-table-output-topic",
                Serdes.String().deserializer(),
                Serdes.Long().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testKTableVsKStreamBehaviorDifference() {
        // given - same sameKey, multiple values
        String sameKey = "sameKey";

        // when
        inputTopic.pipeInput(sameKey, 10L);
        inputTopic.pipeInput(sameKey, 20L);
        inputTopic.pipeInput(sameKey, 30L);

        List<KeyValue<String, Long>> streamResults = streamOutputTopic.readKeyValuesToList();
        List<KeyValue<String, Long>> ktableResults = ktableOutputTopic.readKeyValuesToList();

        // then
        // KStream: processes all records (event log semantics)
        assertEquals(3, streamResults.size());
        assertEquals(10L, streamResults.get(0).value);
        assertEquals(20L, streamResults.get(1).value);
        assertEquals(30L, streamResults.get(2).value);

        // KTable: emits all changes but its state store stores only latest (database table semantics)
        assertEquals(3, ktableResults.size());
        KeyValueStore<String, Long> store = testDriver.getKeyValueStore("kTable-store");
        assertEquals(30L, store.get(sameKey)); // Only latest value in state store

        Set<String> producedTopicNames = testDriver.producedTopicNames();
        // Verify changelog topic behavior: KTable produces to both changelog AND output topic
        // In production, changelog topic backs up the state store for fault tolerance
        assertTrue(producedTopicNames.contains("stream-output-topic"));
        assertTrue(producedTopicNames.contains("k-table-output-topic"));
        assertTrue(producedTopicNames.contains("testKTableVsKStreamApp-kTable-store-changelog"));
        
        // Verify changelog topic content - should contain all state changes
        TestOutputTopic<String, Long> changelogTopic = testDriver.createOutputTopic(
                "testKTableVsKStreamApp-kTable-store-changelog",
                Serdes.String().deserializer(),
                Serdes.Long().deserializer()
        );
        
        List<KeyValue<String, Long>> changelogResults = changelogTopic.readKeyValuesToList();
        assertEquals(3, changelogResults.size()); // All state changes logged
        assertEquals(10L, changelogResults.get(0).value);
        assertEquals(20L, changelogResults.get(1).value);
        assertEquals(30L, changelogResults.get(2).value);
    }

}
