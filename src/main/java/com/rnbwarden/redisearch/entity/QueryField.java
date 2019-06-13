package com.rnbwarden.redisearch.entity;

import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

public class QueryField {

    private SearchableField field;
    private Collection<String> values;
    private SearchOperator operator;

    public QueryField(SearchableField field, String value) {

        this(field, singletonList(value), SearchOperator.INTERSECTION);
    }

    public QueryField(SearchableField field, Collection<String> values, SearchOperator operator) {

        this.field = field;
        this.values = values;
        this.operator = operator;
    }

    public void setField(SearchableField field) {

        this.field = field;
    }

    public void setValues(List<String> values) {

        this.values = values;
    }

    public void setOperator(SearchOperator operator) {

        this.operator = operator;
    }

    public String getName() {

        return field.getName();
    }

    public String getQuerySyntax() {

        Assert.notNull(operator, "SearchOperator cannot be null");
        Assert.notNull(values, "Values cannot be null");

        String queryValueString = values.stream().collect(joining(operator.getJoinString()));
        return field.getQuerySyntax(queryValueString);
    }
}