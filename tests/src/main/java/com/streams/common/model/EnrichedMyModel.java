package com.streams.common.model;

import java.util.Objects;

public class EnrichedMyModel {
    private String name;
    private Double someEnrichedData;

    public EnrichedMyModel() {}

    public EnrichedMyModel(String name, Double someEnrichedData) {
        this.name = name;
        this.someEnrichedData = someEnrichedData;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Double getSomeEnrichedData() { return someEnrichedData; }
    public void setSomeEnrichedData(Double someEnrichedData) { this.someEnrichedData = someEnrichedData; }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedMyModel that = (EnrichedMyModel) o;
        return Objects.equals(name, that.name) && Objects.equals(someEnrichedData, that.someEnrichedData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, someEnrichedData);
    }

    @Override
    public String toString() {
        return "AnotherMyModel{" +
                "name='" + name + '\'' +
                ",\n someEnrichedData=" + someEnrichedData +
                '}';
    }
}

