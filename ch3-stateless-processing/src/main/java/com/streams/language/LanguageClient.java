package com.streams.language;

import com.magicalpipelines.com.EntitySentiment;
import com.streams.serialization.Tweet;

import java.util.List;

public interface LanguageClient {
    Tweet translate(Tweet tweet, String targetLanguage);
    List<EntitySentiment> getEntitySentiment(Tweet tweet);
}
