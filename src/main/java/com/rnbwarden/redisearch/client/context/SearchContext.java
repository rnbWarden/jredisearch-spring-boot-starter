package com.rnbwarden.redisearch.client.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.rnbwarden.redisearch.entity.QueryField;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.SearchableField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SearchContext<E> {

    public static final Integer DEFAULT_MAX_LIMIT_VALUE = 1000000;

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
    private Integer limit = null;
    @Builder.Default
    private List<QueryField<E>> queryFields = new ArrayList<>();
    private List<String> resultFields;

    public void addField(SearchableField<E> field, String value) {

        queryFields.add(new QueryField<>(field, value));
    }

    public void addField(SearchableField<E> field, String value, boolean negated) {

        queryFields.add(new QueryField<>(field, value, negated));
    }

    public void addField(SearchableField<E> field, String value, boolean negated, boolean prefix) {

        queryFields.add(new QueryField<>(field, value, negated, prefix));
    }

    public void addField(SearchableField<E> field, String... values) {

        addField(field, Stream.of(values).collect(Collectors.toList()));
    }

    public void addField(SearchableField<E> field, boolean negated, String... values) {

        addField(field, Stream.of(values).collect(Collectors.toList()), negated);
    }

    public void addField(SearchableField<E> field, Collection<String> values) {

        addField(field, SearchOperator.UNION, values);
    }

    public void addField(SearchableField<E> field, Collection<String> values, boolean negated) {

        addField(field, SearchOperator.UNION, values, negated);
    }

    public void addField(SearchableField<E> field, Collection<String> values, boolean negated, boolean prefix) {

        addField(field, SearchOperator.UNION, values, negated, prefix);
    }

    public void addField(SearchableField<E> field, SearchOperator operator, String... values) {

        addField(field, operator, Stream.of(values).collect(Collectors.toList()));
    }

    public void addField(SearchableField<E> field, SearchOperator operator, Collection<String> values) {

        queryFields.add(new QueryField<>(field, values, operator));
    }

    public void addField(SearchableField<E> field, SearchOperator operator, Collection<String> values, boolean negated) {

        queryFields.add(new QueryField<>(field, values, operator, negated));
    }

    public void addField(SearchableField<E> field, SearchOperator operator, Collection<String> values, boolean negated, boolean prefix) {

        queryFields.add(new QueryField<>(field, values, operator, negated, prefix));
    }

    public void addResultField(String fieldName) {

        if (resultFields == null) {
            resultFields = new ArrayList<>();
        }
        resultFields.add(fieldName);
    }

    public void addResultFields(Collection<String> fieldNames) {

        fieldNames.forEach(this::addResultField);
    }

    public void addResultFields(String... fieldNames) {

        Stream.of(fieldNames).forEach(this::addResultField);
    }

}