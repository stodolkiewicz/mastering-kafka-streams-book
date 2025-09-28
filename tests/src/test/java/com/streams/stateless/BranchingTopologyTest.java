package com.streams.stateless;

import com.streams.model.MyModel;
import com.streams.serde.JsonDeserializer;
import com.streams.serde.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class BranchingTopologyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MyModel> inputTopic;
    private TestOutputTopic<String, MyModel> outputTopicUppercase;
    private TestOutputTopic<String, MyModel> outputTopicLowercase;
    private TestOutputTopic<String, MyModel> outputTopicDefault;

    @BeforeEach
    void setup() {
        Topology branchingTopology = BranchingTopology.createBranchingTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testBranchingTopologyApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");;

        testDriver = new TopologyTestDriver(branchingTopology, props);

        inputTopic = testDriver.createInputTopic(
                "input-topic",
                Serdes.String().serializer(),
                new JsonSerializer<>()
        );

        outputTopicUppercase = testDriver.createOutputTopic(
                "output-topic-uppercase",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        outputTopicLowercase = testDriver.createOutputTopic(
                "output-topic-lowercase",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );

        outputTopicDefault = testDriver.createOutputTopic(
                "output-topic-default",
                Serdes.String().deserializer(),
                new JsonDeserializer<>(MyModel.class)
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testSplitBranch_ShouldCorrectlySplitInto3Branches() {
        // given
        MyModel uppercaseModel = new MyModel();
        uppercaseModel.setName("Uppercase");

        MyModel lowercaseModel = new MyModel();
        lowercaseModel.setName("lowercase");

        MyModel neitherLowerNorUpperCaseModel = new MyModel();
        neitherLowerNorUpperCaseModel.setName("@-I-go-to-default-branch");

        // when
        inputTopic.pipeInput("1", uppercaseModel);
        inputTopic.pipeInput("2", lowercaseModel);
        inputTopic.pipeInput("3", neitherLowerNorUpperCaseModel);

        List<TestRecord<String, MyModel>> upperCaseCaseRecords = outputTopicUppercase.readRecordsToList();
        List<TestRecord<String, MyModel>> lowerCaseRecords = outputTopicLowercase.readRecordsToList();
        List<TestRecord<String, MyModel>> neitherLowerNorUpperCaseRecords = outputTopicDefault.readRecordsToList();

        // then
        assertEquals(1, lowerCaseRecords.size());
        assertEquals(1, upperCaseCaseRecords.size());
        assertEquals(1, neitherLowerNorUpperCaseRecords.size());

        assertEquals(uppercaseModel, upperCaseCaseRecords.getFirst().value());
        assertEquals(lowercaseModel, lowerCaseRecords.getFirst().value());
        assertEquals(neitherLowerNorUpperCaseModel, neitherLowerNorUpperCaseRecords.getFirst().value());
    }
}