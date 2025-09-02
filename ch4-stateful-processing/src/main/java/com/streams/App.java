package com.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

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
        Topology topology = StatefulTopology.createTopology();

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-processing-app");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(2));
                countDownLatch.countDown();
            }));

            try {
                streams.start();
                countDownLatch.await();
            } catch (Exception e) {
                System.exit(1);
            }

        }
        System.exit(0);
    }
}
