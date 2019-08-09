package com.rnbwarden.redisearch.client.context;

import com.rnbwarden.redisearch.entity.QueryField;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.SearchableField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SearchContext {

    public static final Long DEFAULT_MAX_LIMIT_VALUE = 1000000L;

    private boolean noContent;
    private boolean verbatim;
    private boolean noStopWords;
    private boolean withScores;
    private boolean withPayloads;
    private boolean withSortKeys;
    private String language;
    private String sortBy;
    @Builder.Default
    private boolean sortAscending = true;
    @Builder.Default
    private long offset = 0L;
    @Builder.Default
    private long limit = DEFAULT_MAX_LIMIT_VALUE;
    @Builder.Default
    private List<QueryField> queryFields = new ArrayList<>();

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