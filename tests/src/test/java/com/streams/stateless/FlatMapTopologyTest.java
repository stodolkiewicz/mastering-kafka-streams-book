package com.streams.stateless;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonDeserializer;
import com.streams.common.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/*
 * Tests FlatMapTopology methods: flatMap() and flatMapValues()
 * 
 * flatMap() - 1:N transformation, can change key and value
 * flatMapValues() - 1:N transformation, only changes values
 */
class FlatMapTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, MyModel> outputTopic;

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    // === FlatMap Tests ===
    
    @Test
    void testFlatMap_shouldSplitNameIntoMultipleRecordsWithNewKeys() {
        // given
        final String INPUT_KEY = "original-key";
        final String INPUT_NAME = "John Doe Smith";
        final String[] EXPECTED_KEYS = {"john", "doe", "smith"};
        final String[] EXPECTED_NAMES = {"John", "Doe", "Smith"};
        
        setupTopology(FlatMapTopology.createFlatMapTopology());
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        assertEquals(3, results.size());
        
        for (int i = 0; i < 3; i++) {
            assertEquals(EXPECTED_KEYS[i], results.get(i).key);
            assertEquals(EXPECTED_NAMES[i], results.get(i).value.getName());
        }
    }

    @Test
    void testFlatMap_shouldProduceZeroRecordsForEmptyName() {
        // given
        final String INPUT_KEY = "key1";
        final String INPUT_NAME = "";
        
        setupTopology(FlatMapTopology.createFlatMapTopology());
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    // === FlatMapValues Tests ===
    
    @Test
    void testFlatMapValues_shouldSplitNameIntoMultipleRecordsKeepingOriginalKey() {
        // given
        final String INPUT_KEY = "original-key";
        final String INPUT_NAME = "Anna Maria";
        final String[] EXPECTED_NAMES = {"Anna", "Maria"};
        
        setupTopology(FlatMapTopology.createFlatMapValuesTopology());
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        assertEquals(2, results.size());
        
        // All records should keep original key
        for (int i = 0; i < 2; i++) {
            assertEquals(INPUT_KEY, results.get(i).key);
            assertEquals(EXPECTED_NAMES[i], results.get(i).value.getName());
        }
    }

    @Test
    void testFlatMapValues_shouldProduceZeroRecordsForNullName() {
        // given
        final String INPUT_KEY = "key1";
        
        setupTopology(FlatMapTopology.createFlatMapValuesTopology());
        
        MyModel input = new MyModel();
        input.setName(null);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private void setupTopology(Topology topology) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFlatMapTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
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
}