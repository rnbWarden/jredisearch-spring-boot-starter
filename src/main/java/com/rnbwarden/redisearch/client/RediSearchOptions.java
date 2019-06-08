package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.QueryField;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.SearchableField;
import lombok.Data;

import java.util.*;

import static java.util.stream.Collectors.joining;

@Data
public class RediSearchOptions {

    public static Long defaultMaxValue = Long.MAX_VALUE;
    private List<QueryField> queryFields = new ArrayList<>();

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

        queryFields.add(new QueryField(field, value));
    }

    public void addField(SearchableField field, SearchOperator operator, String... values) {

        queryFields.add(new QueryField(field, Arrays.asList(values), operator));
    }

    public void addField(SearchableField field, SearchOperator operator, Collection<String> values) {

        queryFields.add(new QueryField(field, values, operator));
    }
}