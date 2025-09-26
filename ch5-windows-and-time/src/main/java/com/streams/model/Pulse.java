package com.streams.model;

public class Pulse implements Vital{
    private String timestamp;

    @Override
    public String getTimestamp() {
        return timestamp;
    }
}
