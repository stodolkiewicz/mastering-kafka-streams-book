package com.streams.stateless;

import com.streams.model.MyModel;
import com.streams.serde.JsonDeserializer;
import com.streams.serde.JsonSerializer;
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
 * Tests MergeTopology method: merge()
 * 
 * merge() - combines multiple input streams into single output
 * Simple union operation without transformation
 */
class MergeTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic1;
    private TestInputTopic<String, MyModel> inputTopic2;
    private TestOutputTopic<String, MyModel> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMergeTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = MergeTopology.createMergeTopology();
        testDriver = new TopologyTestDriver(topology, props);

        inputTopic1 = testDriver.createInputTopic(
                "input-topic-1",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        inputTopic2 = testDriver.createInputTopic(
                "input-topic-2",
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
    void testMerge_shouldMergeRecordsFromBothTopics() {
        // given
        final String NAME_FROM_TOPIC1 = "Record1";
        final String NAME_FROM_TOPIC2 = "Record2";
        
        MyModel model1 = new MyModel();
        model1.setName(NAME_FROM_TOPIC1);
        
        MyModel model2 = new MyModel();
        model2.setName(NAME_FROM_TOPIC2);

        // when
        inputTopic1.pipeInput("key1", model1);
        inputTopic2.pipeInput("key2", model2);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        assertEquals(2, results.size());
    }
}