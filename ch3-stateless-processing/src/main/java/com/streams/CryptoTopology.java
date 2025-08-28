package com.streams;

import com.magicalpipelines.com.EntitySentiment;
import com.streams.language.DummyLanguageClient;
import com.streams.serialization.Tweet;
import com.streams.serialization.avro.sr.AvroSchemaAwareSerdes;
import com.streams.serialization.json.TweetSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CryptoTopology {
    private static final List<String> currencies = Arrays.asList("bitcoin", "ethereum");

    public static Topology createTopology() {
        DummyLanguageClient languageClient = new DummyLanguageClient();
        Predicate<byte[], Tweet> englishTweets = (key, tweet) -> tweet.getLang().equals("en");
        Predicate<byte[], Tweet> nonEnglishTweets = (key, tweet) -> !tweet.getLang().equals("en");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<byte[], Tweet> stream = builder.stream(
                "tweets",
                Consumed.with(
                        Serdes.ByteArray(),
                        new TweetSerdes()
                )
        )
        .peek((key, value) -> System.out.println("DUPA1 " +key + ": " + value))
        .filterNot(
                (key, tweet) -> tweet.isRetweet()
        ).peek((key, value) -> System.out.println("DUPA2 " +key + ": " + value));

        // Split to english, non-english branches ----------------------------------------------------------------
        Map<String, KStream<byte[], Tweet>> languageSplitKStreams = stream.split(Named.as("language-split-"))
                .branch(englishTweets, Branched.as("english"))
                .branch(nonEnglishTweets, Branched.as("non-english"))
                .noDefaultBranch();

        KStream<byte[], Tweet> nonEnglishStream = languageSplitKStreams.get("language-split-non-english");
        KStream<byte[], Tweet> englishStream = languageSplitKStreams.get("language-split-english");

        nonEnglishStream.mapValues(tweet -> languageClient.translate(tweet, "en"))
                .peek((key, value) -> System.out.println("DUPA3 PRZETŁUMACZONA " +key + ": " + value));

        // Merge english, non-english branches -------------------------------------------------------------------
        KStream<byte[], Tweet> mergedStream = englishStream.merge(nonEnglishStream);

        // sentiment ---------------------------------------------------------------------------------------------
        KStream<byte[], EntitySentiment> entitySentimentKStream = mergedStream.flatMapValues(tweet -> {
            List<EntitySentiment> results = languageClient.getEntitySentiment(tweet);

            results.removeIf(entitySentiment -> !currencies.contains(entitySentiment.getEntity()));

            return results;
        })
        .peek((key, value) -> System.out.println("DUPA4 sentymentalna " +key + ": " + value));

        Class<EntitySentiment> clazz = EntitySentiment.class;

        entitySentimentKStream.to(
                "crypto-sentiment",
                Produced.with(
                        Serdes.ByteArray(),
                        // because windows...
                        AvroSchemaAwareSerdes.getSerde(clazz, "http://host.docker.internal:8081", false)
                )
        );

        return builder.build();
    }
}
