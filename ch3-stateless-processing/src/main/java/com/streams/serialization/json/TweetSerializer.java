package com.streams.serialization.json;

import com.google.gson.Gson;
import com.streams.serialization.Tweet;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class TweetSerializer implements Serializer<Tweet> {
    private Gson gson = new Gson();

    @Override
    public byte[] serialize(String s, Tweet tweet) {
        if (tweet == null) {
            return null;
        }
        return gson.toJson(tweet).getBytes(StandardCharsets.UTF_8);
    }
}
