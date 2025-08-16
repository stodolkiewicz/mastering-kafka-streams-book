package com.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        // PROPERTIES -----------------------------------------------------------------------------
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // KSTREAM & TOPOLOGY ---------------------------------------------------------------------
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Void, String> stream = builder.stream("users");

        stream
                .foreach((k, v) -> System.out.println("(DSL) Hello, " + v));
        Topology topology = builder.build();

        // START ---------------------------------------------------------------------------------
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, config);) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                countDownLatch.countDown();
            }));

            try {
                kafkaStreams.start();
                countDownLatch.await();
            } catch (Exception e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
