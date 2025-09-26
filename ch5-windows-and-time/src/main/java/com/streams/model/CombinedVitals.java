package com.streams.model;

public class CombinedVitals {
    private final int heartRate;
    private final BodyTemp bodyTemp;

    public CombinedVitals(int heartRate, BodyTemp bodyTemp) {
        this.heartRate = heartRate;
        this.bodyTemp = bodyTemp;
    }

    public int getHeartRate() {
        return heartRate;
    }

    public BodyTemp getBodyTemp() {
        return bodyTemp;
    }

    @Override
    public String toString() {
        return "CombinedVitals{" +
                "heartRate=" + heartRate +
                ", bodyTemp=" + bodyTemp +
                '}';
    }
}
