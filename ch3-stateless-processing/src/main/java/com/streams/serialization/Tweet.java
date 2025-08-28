package com.streams.serialization;

import com.google.gson.annotations.SerializedName;

public class Tweet {
    @SerializedName("CreatedAt")
    private Long createdAt;

    @SerializedName("Lang")
    private String lang;

    @SerializedName("Retweet")
    private Boolean retweet;

    @SerializedName("Text")
    private String text;

    @SerializedName("Id")
    private Long id;

    public Tweet() {
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean isRetweet() {
        return retweet;
    }

    public void setRetweet(Boolean retweet) {
        this.retweet = retweet;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "createdAt=" + createdAt +
                ", lang='" + lang + '\'' +
                ", retweet=" + retweet +
                ", text='" + text + '\'' +
                ", id=" + id +
                '}';
    }
}
