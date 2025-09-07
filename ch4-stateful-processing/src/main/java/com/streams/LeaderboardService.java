package com.streams;

import com.streams.model.HighScores;
import io.javalin.Javalin;
import io.javalin.http.Context;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderboardService {
    private final HostInfo hostInfo;
    private final KafkaStreams streams;

    private static final Logger log = LoggerFactory.getLogger(LeaderboardService.class);

    public LeaderboardService(HostInfo hostInfo, KafkaStreams streams) {
        this.hostInfo = hostInfo;
        this.streams = streams;
    }

    public ReadOnlyKeyValueStore<String, HighScores>  getStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        "leader-boards",
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    void start() {
        Javalin app = Javalin.create().start(hostInfo.port());
        System.out.println(hostInfo.port());
        app.get("/leaderboard/{key}", this::getKey);
    }

    void getKey(Context context) {
        String productId = context.pathParam("key");

        KeyQueryMetadata metadata = streams.queryMetadataForKey(
                "leader-boards", productId, Serdes.String().serializer()
        );


        // Nawet jeśli rekord nigdy nie istniał, metadata wskaże gdzie powinien siedzieć, gdyby istniał.

        // if key on this instance
        if(hostInfo.equals(metadata.activeHost())) {
            HighScores highScores = getStore().get(productId);

            if(highScores == null ) {
                context.status(404);
                return;
            }

            // send response as json
            context.json(highScores.toList());
            return;
        }

        String remoteHost = metadata.activeHost().host();
        int remotePort = metadata.activeHost().port();

        String url = String.format("http://%s:%d/leaderboard/%s",
                remoteHost, remotePort, productId);

        System.out.println("URL: " + url);

        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request.Builder().url(url).build();

        // call the second instance to get the response
        try (Response response = okHttpClient.newCall(request).execute()) {
            context.result(response.body().string());
        } catch (Exception e) {
            context.status(500);
        }

    }

    void getCount(Context ctx) {
        // Initialize the count with the number of entries in the local state store
        long count = getStore().approximateNumEntries();

        for (StreamsMetadata metadata : streams.streamsMetadataForStore("leader-boards")) {
            if (!hostInfo.equals(metadata.hostInfo())) {
                continue;
            }
            count += fetchCountFromRemoteInstance(metadata.hostInfo().host(), metadata.hostInfo().port());
        }

        ctx.json(count);
    }

    long fetchCountFromRemoteInstance(String host, int port) {
        OkHttpClient client = new OkHttpClient();

        String url = String.format("http://%s:%d/leaderboard/count/local", host, port);
        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            return Long.parseLong(response.body().string());
        } catch (Exception e) {
            // log error
            log.error("Could not get leaderboard count", e);
            return 0L;
        }
    }

}
