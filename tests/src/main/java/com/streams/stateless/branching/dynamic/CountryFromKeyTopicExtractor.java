package com.streams.stateless.branching.dynamic;

import com.streams.model.MyModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

/*
 * TopicNameExtractor determines (and creates if needed) topic names dynamically
 * 
 * WHY USE THIS:
 * - Sometimes you don't know topic names upfront and need to route records based on actual data content
 * 
 * EXAMPLE:
 * - Country from key: "USA" -> topic "USA" 
 * - Missing data -> topic "unknown country"
 */
public class CountryFromKeyTopicExtractor implements TopicNameExtractor<String, MyModel> {
    @Override
    public String extract(String key, MyModel myModel, RecordContext recordContext) {
        return StringUtils.isNotBlank(key) ? key : "unknown-country";
    }
}
