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
 * Tests FilterTopology method: filter()
 * 
 * filter() - keeps or drops records based on predicate, doesn't modify data
 */
class FilterTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, MyModel> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = FilterTopology.createFilterTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFilterTopologyApp");
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

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testFilter_shouldNotFilterModelWithIntFieldValuesOfMoreThan10() {
        // given
        MyModel myModel = new MyModel();
        myModel.setIntField(11);

        // when
        inputTopic.pipeInput("key-1", myModel);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<MyModel> myModels = outputTopic.readValuesToList();
        MyModel outputModel = myModels.getFirst();

        assertEquals(outputModel, myModel);
    }

    @Test
    void testFilter_shouldFilterModelWithIntFieldValuesOfMoreThan10() {
        // given
        MyModel myModel = new MyModel();
        myModel.setIntField(8);

        // when
        inputTopic.pipeInput(myModel);

        // then
        assertThat(outputTopic.isEmpty()).isTrue();
    }

}