package com.streams;

import com.streams.serialization.Tweet;
import com.streams.serialization.json.TweetDeserializer;
import com.streams.serialization.json.TweetSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCharSequence;

//https://kafka.apache.org/40/documentation/streams/developer-guide/testing.html

public class AppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], Tweet> inputTopic;
//    private TestOutputTopic<Void, String> outputTopic;

    @BeforeEach
    void setup() {
        Topology topology = CryptoTopology.createTopology();
        Serde<Tweet> tweetSerde = new TweetSerdes();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-processing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-123");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TweetSerdes.class);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "tweets",
                null,
                tweetSerde.serializer()
        );

//        outputTopic = testDriver.createOutputTopic(
//                "outputTopic",
//                Serdes.Void().deserializer(),
//                Serdes.String().deserializer()
//        );
    }

//    @Test
//    void testUserIsGreeted() {
//        // given
//        String value = "Dawid";
//        String EXPECTED_OUTPUT = "Hello Dawid!";
//
//        // when
//        inputTopic.pipeInput(null, value);
//
//        // then
//        assertThat(outputTopic.isEmpty()).isFalse();
//
//        List<TestRecord<Void, String>> outRecords = outputTopic.readRecordsToList();
//
//        assertThat(outRecords).hasSize(1);
//        assertThatCharSequence(outRecords.get(0).value()).isEqualTo(EXPECTED_OUTPUT);
//    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }
}
