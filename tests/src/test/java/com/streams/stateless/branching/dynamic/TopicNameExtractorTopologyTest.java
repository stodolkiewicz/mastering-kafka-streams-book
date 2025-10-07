package com.streams.stateless.branching.dynamic;

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
 * Tests TopicNameExtractorTopology method: to() with TopicNameExtractor
 * 
 * TopicNameExtractor - routes records to different topics based on data content
 * Creates topics dynamically instead of using fixed topic names
 */
class TopicNameExtractorTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testTopicNameExtractorApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = TopicNameExtractorTopology.createTopicNameExtractorTopology();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testTopicNameExtractor_shouldRouteToTopicNamedAfterKey() {
        // given
        final String INPUT_KEY = "USA";
        final String INPUT_NAME = "test-model";
        final String EXPECTED_TOPIC = "USA";
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // in real environment, kafka streams creates this topic
        TestOutputTopic<String, MyModel> outputTopic = testDriver.createOutputTopic(
                EXPECTED_TOPIC,
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertEquals(INPUT_KEY, result.key);
        assertEquals(INPUT_NAME, result.value.getName());
    }

    @Test
    void testTopicNameExtractor_shouldRouteToUnknownCountryWhenKeyIsNull() {
        // given
        final String INPUT_KEY = null;
        final String INPUT_NAME = "test-model";
        final String EXPECTED_TOPIC = "unknown-country";
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // in real environment, kafka streams creates this topic
        TestOutputTopic<String, MyModel> outputTopic = testDriver.createOutputTopic(
                EXPECTED_TOPIC,
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertNull(result.key);
        assertEquals(INPUT_NAME, result.value.getName());
    }

    @Test
    void testTopicNameExtractor_shouldRouteToUnknownCountryWhenKeyIsEmpty() {
        // given
        final String INPUT_KEY = "";
        final String INPUT_NAME = "test-model";
        final String EXPECTED_TOPIC = "unknown-country";
        
        MyModel input = new MyModel();
        input.setName(INPUT_NAME);

        // in real environment, kafka streams creates this topic
        TestOutputTopic<String, MyModel> outputTopic = testDriver.createOutputTopic(
                EXPECTED_TOPIC,
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        // when
        inputTopic.pipeInput(INPUT_KEY, input);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        KeyValue<String, MyModel> result = results.getFirst();

        assertEquals(INPUT_KEY, result.key);
        assertEquals(INPUT_NAME, result.value.getName());
    }
}