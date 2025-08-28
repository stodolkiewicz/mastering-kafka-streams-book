package com.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCharSequence;

//https://kafka.apache.org/40/documentation/streams/developer-guide/testing.html

public class AppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<Void, String> inputTopic;
    private TestOutputTopic<Void, String> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = App.createTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "users",
                Serdes.Void().serializer(),
                Serdes.String().serializer()
        );

        outputTopic = testDriver.createOutputTopic(
                "outputTopic",
                Serdes.Void().deserializer(),
                Serdes.String().deserializer()
        );
    }

    @Test
    void testUserIsGreeted() {
        // given
        String value = "Dawid";
        String EXPECTED_OUTPUT = "Hello Dawid!";

        // when
        inputTopic.pipeInput(null, value);

        // then
        assertThat(outputTopic.isEmpty()).isFalse();

        List<TestRecord<Void, String>> outRecords = outputTopic.readRecordsToList();

        assertThat(outRecords).hasSize(1);
        assertThatCharSequence(outRecords.get(0).value()).isEqualTo(EXPECTED_OUTPUT);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }
}
