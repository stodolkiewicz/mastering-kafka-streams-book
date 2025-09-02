package com.streams;

import com.streams.model.*;
import com.streams.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class StatefulTopology {

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, ScoreEvent> scoreEvents = builder.stream(
                "score-events",
                Consumed.with(Serdes.ByteArray(), new JsonSerDes<>(ScoreEvent.class))
        )
        .selectKey((k,v) -> v.getPlayerId().toString());

        KTable<String, Player> players = builder.table(
                "players",
                Consumed.with(Serdes.String(), new JsonSerDes<>(Player.class))
        );

        GlobalKTable<String, Product> products = builder.globalTable(
                "products",
                Consumed.with(Serdes.String(), new JsonSerDes<>(Product.class))
        );

        // JOIN score-events and players --------------------------------------------------------------------------
        ValueJoiner<ScoreEvent, Player, ScoreWithPlayer> scorePlayerJoiner = (ScoreWithPlayer::new);
        Joined<String, ScoreEvent, Player> playerJoinParams =
                Joined.with(Serdes.String(), new JsonSerDes<>(ScoreEvent.class), new JsonSerDes<>(Player.class));

        KStream<String, ScoreWithPlayer> scoreWithPlayer = scoreEvents.join(
                players,
                scorePlayerJoiner,
                playerJoinParams
        );

        // join products -------------------------------------------------------------------------------------------
        KeyValueMapper<String, ScoreWithPlayer, String> keyMapper = (leftKey, scorePlayer) -> {
            return String.valueOf(scorePlayer.getScoreEvent().getProductId());
        };

        ValueJoiner<ScoreWithPlayer, Product, Enriched> productJoiner =
                (scoreWithPlayerVar, product) -> new Enriched(scoreWithPlayerVar, product);

        KStream<String, Enriched> withProducts = scoreWithPlayer.join(
                products,
                keyMapper,
                productJoiner
        );

        // groupBy records -----------------------------------------------------------------------------------------

        // groupBy will rekey records.
        //        Since we want to calculate the high scores for each product ID, and
        //        since our enriched stream is currently keyed by player ID
        KGroupedStream<String, Enriched> grouped =
            withProducts.groupBy(
                (key, enriched) -> enriched.getProductId().toString(), // selecting key here
                Grouped.with(Serdes.String(), new JsonSerDes<>(Enriched.class))
        );
        // KGroupedStream is just an intermediate
        // representation of a stream that allows us to perform aggregations.


        // no rekeying
        /*
        KGroupedStream<String, Enriched> grouped =
                withProducts.groupByKey(
                        Grouped.with(Serdes.String(), new JsonSerDes<>(Enriched.class))
                );
        */

        // aggregation ----------------------------------------------------------------------------------------------
        //        At a high level, aggregations are just a way of combining multiple
        //        input values into a single output value.
        Initializer<Long> countInitializer = () -> 0L;

        Initializer<HighScores> highScoresInitializer = HighScores::new;

        Aggregator<String, Enriched, HighScores> highScoresAdder =
                (key, value, currentAggregateValue) -> currentAggregateValue.add(value);

        KTable<String, HighScores> highScores = grouped.aggregate(
                highScoresInitializer,
                highScoresAdder,
                Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>as("leader-boards")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new JsonSerDes<>(HighScores.class))
        );

        // table aggregation ---
        KGroupedTable<String, Player> groupedPlayers =
                players.groupBy(
                        (key, value) -> KeyValue.pair(key, value),
                        Grouped.with(Serdes.String(), new JsonSerDes<>(Player.class))
                );

        groupedPlayers.aggregate(
                () -> 0L,
                (key, value, aggregate) -> aggregate + 1L,
                (key, value, aggregate) -> aggregate - 1L);


        return builder.build();
    }
}
