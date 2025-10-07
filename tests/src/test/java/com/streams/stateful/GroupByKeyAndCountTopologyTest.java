package com.streams.stateful;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class GroupByKeyAndCountTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @BeforeEach
    void setup() {
        Topology topology = GroupByKeyAndCountTopology.createGroupByKeyAndCountTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testGroupByKeyAndCountTopolog");
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
                Serdes.Long().deserializer()
        );
    }

    @Test
    void test() {
        // given
        MyModel myModel1 = new MyModel();
        MyModel myModel2 = new MyModel();
        MyModel myModel3 = new MyModel();

        // when
        inputTopic.pipeInput("key-1", myModel1);
        inputTopic.pipeInput("key-2", myModel2);
        inputTopic.pipeInput("key-2", myModel3);

        // then
        Map<String, Long> outputMap = outputTopic.readKeyValuesToMap();
        assertEquals(2, outputMap.size());

        Long ValueForKey1 = outputMap.get("key-1");
        Long ValueForKey2 = outputMap.get("key-2");

        assertEquals(1, ValueForKey1);
        assertEquals(2, ValueForKey2);
    }
}