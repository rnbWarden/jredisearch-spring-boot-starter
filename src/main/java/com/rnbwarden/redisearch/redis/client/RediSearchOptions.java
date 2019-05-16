package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.redis.entity.SearchableField;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class RediSearchOptions {

    public static Long defaultMaxValue = Long.MAX_VALUE;
    private Map<SearchableField, String> fieldNameValues = new HashMap<>();

    protected boolean noContent;
    protected boolean verbatim;
    protected boolean noStopWords;
    protected boolean withScores;
    protected boolean withPayloads;
    protected boolean withSortKeys;
    protected String language;
    protected String sortBy;
    protected Long offset;
    protected Long limit;

    public void addField(SearchableField field, String value) {

        fieldNameValues.put(field, value);
    }

}