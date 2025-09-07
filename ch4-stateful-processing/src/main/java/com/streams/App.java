package com.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;

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

        String host = "localhost";
        int port = Integer.parseInt(System.getProperty("server.port"));
        String stateDir = System.getProperty("state.dir");

        String endpoint = String.format("%s:%s", host, port);

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-processing-app");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        // określa ścieżkę do katalogu, w którym aplikacja Kafka Streams będzie przechowywać swoje lokalne magazyny stanu.
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            CountDownLatch countDownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(2));
                countDownLatch.countDown();
            }));

            try {
                streams.start();

                // start the REST service
                HostInfo hostInfo = new HostInfo(host, port);
                LeaderboardService service = new LeaderboardService(hostInfo, streams);
                service.start();

                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }

        }
        System.exit(0);
    }
}
