package com.streams.stateless;

import com.streams.common.model.EnrichedMyModel;
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
 * Tests MapTopology methods: map() and mapValues()
 * 
 * map() - transforms both key and value, can change types
 * mapValues() - transforms only values, key unchanged
 */
class MapTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMapTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testMap_shouldTransformMyModelToEnrichedMyModel() {
        // given
        final String INPUT_KEY = "test-key";
        final Integer MY_MODEL_INT_VALUE = 10;
        final Double MY_MODEL_DOUBLE_VALUE = 5.5;

        final String EXPECTED_KEY = "test-key_enriched";
        final Double EXPECTED_ENRICHED_DATA = 15.5;
        
        Topology topology = MapTopology.createMapTopology();
        testDriver = new TopologyTestDriver(topology, setupProps());

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        TestOutputTopic<String, EnrichedMyModel> outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(EnrichedMyModel.class)
        );

        MyModel input = new MyModel();
        input.setIntField(MY_MODEL_INT_VALUE);
        input.setDoubleField(MY_MODEL_DOUBLE_VALUE);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, EnrichedMyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, EnrichedMyModel> result = results.getFirst();

        assertEquals(EXPECTED_KEY, result.key);
        assertEquals(EXPECTED_ENRICHED_DATA, result.value.getSomeEnrichedData());
    }

    @Test
    void testMapValues_shouldTransformOnlyValues() {
        // given
        final String INPUT_KEY = "original-key";
        final String INPUT_NAME = "test";
        final Integer INPUT_INT_VALUE = 10;

        final String EXPECTED_NAME = "TEST";
        final Integer EXPECTED_INT_VALUE = 20;
        
        Topology topology = MapTopology.createMapValuesTopology();
        testDriver = new TopologyTestDriver(topology, setupProps());

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        TestOutputTopic<String, MyModel> outputTopic = testDriver.createOutputTopic(
                "output-topic",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        MyModel input = new MyModel();
        input.setName(INPUT_NAME);
        input.setIntField(INPUT_INT_VALUE);

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertEquals(INPUT_KEY, result.key); // key unchanged
        assertEquals(EXPECTED_NAME, result.value.getName()); // name to uppercase
        assertEquals(EXPECTED_INT_VALUE, result.value.getIntField()); // intField * 2
    }

    private Properties setupProps() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMapTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        return props;
    }
}