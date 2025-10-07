package com.streams.stateful;

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
 * Tests GroupByAndReduceTopology methods: groupBy() + reduce()
 * 
 * groupBy() - triggers repartitioning when changing key
 * reduce() - stateful operation maintaining running totals per key
 */
class GroupByAndReduceTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, MyModel> outputTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testGroupByAndReduceApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        
        Topology topology = GroupByAndReduceTopology.createGroupByAndReduceTopology();
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
    void testGroupByAndReduce_shouldSumDoubleFieldForSameName() {
        // given
        final String ORIGINAL_KEY1 = "user123";
        final String ORIGINAL_KEY2 = "user456";
        final String NAME = "Alice";
        final Double VALUE1 = 10.5;
        final Double VALUE2 = 5.5;
        final Double EXPECTED_SUM = 16.0;
        
        MyModel model1 = new MyModel();
        model1.setName(NAME);
        model1.setDoubleField(VALUE1);
        
        MyModel model2 = new MyModel();
        model2.setName(NAME);
        model2.setDoubleField(VALUE2);

        // when
        inputTopic.pipeInput(ORIGINAL_KEY1, model1);
        inputTopic.pipeInput(ORIGINAL_KEY2, model2);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        // Should have 2 results: initial and updated sum
        assertEquals(2, results.size());
        
        // First result: initial value
        assertEquals(NAME, results.get(0).key);
        assertEquals(VALUE1, results.get(0).value.getDoubleField());
        
        // Second result: reduced sum
        assertEquals(NAME, results.get(1).key);
        assertEquals(EXPECTED_SUM, results.get(1).value.getDoubleField());
        assertEquals(NAME, results.get(1).value.getName());
    }

    @Test
    void testGroupByAndReduce_shouldHandleDifferentNames() {
        // given
        final String NAME_ALICE = "Alice";
        final String NAME_BOB = "Bob";
        final Double ALICE_VALUE = 10.0;
        final Double BOB_VALUE = 20.0;
        final Double ALICE_VALUE2 = 7.0;
        final Double EXPECTED_ALICE_SUM = 17.0;
        
        MyModel aliceModel1 = new MyModel();
        aliceModel1.setName(NAME_ALICE);
        aliceModel1.setDoubleField(ALICE_VALUE);
        
        MyModel bobModel = new MyModel();
        bobModel.setName(NAME_BOB);
        bobModel.setDoubleField(BOB_VALUE);
        
        MyModel aliceModel2 = new MyModel();
        aliceModel2.setName(NAME_ALICE);
        aliceModel2.setDoubleField(ALICE_VALUE2);

        // when
        inputTopic.pipeInput("key1", aliceModel1);
        inputTopic.pipeInput("key2", bobModel);
        inputTopic.pipeInput("key3", aliceModel2);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        assertEquals(3, results.size());
        
        // First Alice record
        assertEquals(NAME_ALICE, results.get(0).key);
        assertEquals(ALICE_VALUE, results.get(0).value.getDoubleField());
        
        // Bob record (separate group)
        assertEquals(NAME_BOB, results.get(1).key);
        assertEquals(BOB_VALUE, results.get(1).value.getDoubleField());
        
        // Alice updated sum
        assertEquals(NAME_ALICE, results.get(2).key);
        assertEquals(EXPECTED_ALICE_SUM, results.get(2).value.getDoubleField());
    }

    @Test
    void testGroupByAndReduce_shouldUseUnknownForNullName() {
        // given
        final String EXPECTED_KEY = "UNKNOWN";
        final Double VALUE1 = 25.0;
        final Double VALUE2 = 3.0;
        final Double EXPECTED_SUM = 28.0;
        
        MyModel model1 = new MyModel();
        model1.setName(null);
        model1.setDoubleField(VALUE1);
        
        MyModel model2 = new MyModel();
        model2.setName(null);
        model2.setDoubleField(VALUE2);

        // when
        inputTopic.pipeInput("key1", model1);
        inputTopic.pipeInput("key2", model2);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();
        List<KeyValue<String, MyModel>> results = outputTopic.readKeyValuesToList();
        
        assertEquals(2, results.size());
        
        // First UNKNOWN record
        assertEquals(EXPECTED_KEY, results.get(0).key);
        assertEquals(VALUE1, results.get(0).value.getDoubleField());
        
        // Second UNKNOWN record - sum
        assertEquals(EXPECTED_KEY, results.get(1).key);
        assertEquals(EXPECTED_SUM, results.get(1).value.getDoubleField());
    }

}