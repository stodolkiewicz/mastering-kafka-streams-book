package com.streams.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MyModel {
    private String name;
    private Integer intField;
    private Double doubleField;
    private Boolean booleanField;
    private MyEnum myEnumField;
    private List<String> stringList;
    private Map<String, Integer> stringIntegerMap;
    private NestedRecord nested;

    public MyModel() {}

    public MyModel(String name, Integer intField, Double doubleField, Boolean booleanField,
                   MyEnum myEnumField, List<String> stringList, Map<String, Integer> stringIntegerMap,
                   NestedRecord nested) {
        this.name = name;
        this.intField = intField;
        this.doubleField = doubleField;
        this.booleanField = booleanField;
        this.myEnumField = myEnumField;
        this.stringList = stringList;
        this.stringIntegerMap = stringIntegerMap;
        this.nested = nested;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getIntField() { return intField; }
    public void setIntField(Integer intField) { this.intField = intField; }

    public Double getDoubleField() { return doubleField; }
    public void setDoubleField(Double doubleField) { this.doubleField = doubleField; }

    public Boolean getBooleanField() { return booleanField; }
    public void setBooleanField(Boolean booleanField) { this.booleanField = booleanField; }

    public MyEnum getMyEnumField() { return myEnumField; }
    public void setMyEnumField(MyEnum myEnumField) { this.myEnumField = myEnumField; }

    public List<String> getStringList() { return stringList; }
    public void setStringList(List<String> stringList) { this.stringList = stringList; }

    public Map<String, Integer> getStringIntegerMap() { return stringIntegerMap; }
    public void setStringIntegerMap(Map<String, Integer> stringIntegerMap) { this.stringIntegerMap = stringIntegerMap; }

    public NestedRecord getNested() { return nested; }
    public void setNested(NestedRecord nested) { this.nested = nested; }

    public static class NestedRecord {
        private String nestedText;
        private Integer nestedInt;

        public NestedRecord() {}

        public NestedRecord(String nestedText, Integer nestedInt) {
            this.nestedText = nestedText;
            this.nestedInt = nestedInt;
        }

        public String getNestedText() { return nestedText; }
        public void setNestedText(String nestedText) { this.nestedText = nestedText; }

        public Integer getNestedInt() { return nestedInt; }
        public void setNestedInt(Integer nestedInt) { this.nestedInt = nestedInt; }
    }

    public enum MyEnum {
        ONE("one"),
        TWO("two"),
        THREE("three");

        private final String code;

        MyEnum(String code) {
            this.code = code;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MyModel myModel = (MyModel) o;
        return Objects.equals(name, myModel.name) && Objects.equals(intField, myModel.intField) && Objects.equals(doubleField, myModel.doubleField) && Objects.equals(booleanField, myModel.booleanField) && myEnumField == myModel.myEnumField && Objects.equals(stringList, myModel.stringList) && Objects.equals(stringIntegerMap, myModel.stringIntegerMap) && Objects.equals(nested, myModel.nested);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, intField, doubleField, booleanField, myEnumField, stringList, stringIntegerMap, nested);
    }

    @Override
    public String toString() {
        return "MyModel{" +
                "name='" + name + '\'' +
                ",\n intField=" + intField +
                ",\n doubleField=" + doubleField +
                ",\n booleanField=" + booleanField +
                ",\n myEnumField=" + myEnumField +
                ",\n stringList=" + stringList +
                ",\n stringIntegerMap=" + stringIntegerMap +
                ",\n nested=" + nested +
                '}';
    }
}
