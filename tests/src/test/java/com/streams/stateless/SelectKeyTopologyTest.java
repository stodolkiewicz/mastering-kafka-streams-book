package com.streams.stateless;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonDeserializer;
import com.streams.common.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/*
 * Tests SelectKeyTopology method: selectKey()
 * 
 * selectKey() - changes record key based on value content
 */
class SelectKeyTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, MyModel> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testSelectKeyTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = SelectKeyTopology.createSelectKeyTopology();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testSelectKey_shouldChangeKeyToNameValue() {
        // given
        final String ORIGINAL_KEY = "original-key";
        final String INPUT_NAME = "Alice";
        final String EXPECTED_NEW_KEY = "Alice";
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // when
        inputTopic.pipeInput(ORIGINAL_KEY, input);

        // then
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertEquals(EXPECTED_NEW_KEY, result.key);
        assertEquals(INPUT_NAME, result.value.getName());
    }

    @Test
    void testSelectKey_shouldUseUnknownForNullName() {
        // given
        final String ORIGINAL_KEY = "original-key";
        final String EXPECTED_NEW_KEY = "UNKNOWN";
        
        MyModel input = new MyModel();
        input.setName(null);

        // when
        inputTopic.pipeInput(ORIGINAL_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertEquals(EXPECTED_NEW_KEY, result.key);
        assertNull(result.value.getName());
    }

    @Test
    void testSelectKey_shouldProcessMultipleRecordsWithDifferentKeys() {
        // given
        final String INPUT_NAME1 = "Alice";
        final String INPUT_NAME2 = "Bob";
        final String INPUT_NAME3 = "Alice"; // Same name as first
        
        MyModel model1 = new MyModel();
        model1.setName(INPUT_NAME1);
        
        MyModel model2 = new MyModel();
        model2.setName(INPUT_NAME2);
        
        MyModel model3 = new MyModel();
        model3.setName(INPUT_NAME3);

        // when
        inputTopic.pipeInput("key1", model1);
        inputTopic.pipeInput("key2", model2);
        inputTopic.pipeInput("key3", model3);

        // then
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        assertEquals(3, results.size());
        
        // All records should have names as keys
        assertEquals("Alice", results.get(0).key);
        assertEquals("Bob", results.get(1).key);
        assertEquals("Alice", results.get(2).key); // Same key as first
        
        assertEquals("Alice", results.get(0).value.getName());
        assertEquals("Bob", results.get(1).value.getName());
        assertEquals("Alice", results.get(2).value.getName());
    }
}