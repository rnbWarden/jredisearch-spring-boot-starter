package com.rnbwarden.redisearch.entity;

import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

public class QueryField<E> {

    private static final String redisSpecialCharactersRegEx = "([,.<>{}\\[\\]\"':;!@#$%^&*()\\-+=~\\\\\\s]|[&|]{2})";

    private SearchableField<E> field;
    private Collection<String> values;
    private SearchOperator operator;
    private boolean negated = false;

    public QueryField(SearchableField<E> field, String value) {

        this(field, singletonList(value), SearchOperator.INTERSECTION);
    }

    public QueryField(SearchableField<E> field, String value, boolean negated) {

        this(field, singletonList(value), SearchOperator.INTERSECTION, negated);
    }

    public QueryField(SearchableField<E> field, Collection<String> values, SearchOperator operator) {
        this.field = field;
        this.values = values;
        this.operator = operator;
    }

    public QueryField(SearchableField<E> field, Collection<String> values, SearchOperator operator, boolean negated) {
        this(field, values, operator);
        this.negated = negated;
    }


    public void setField(SearchableField<E> field) {

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

    public boolean isNegated() {
        return negated;
    }

    public String getQuerySyntax() {

        Assert.notNull(operator, "SearchOperator cannot be null");
        Assert.notNull(values, "Values cannot be null");

        String queryValueString = values.stream()
                .map(QueryField::escapeSpecialCharacters)
                .collect(joining(operator.getJoinString()));
        return field.getQuerySyntax(queryValueString);
    }

    public static String escapeSpecialCharacters(String s) {

        return s.replaceAll(redisSpecialCharactersRegEx, "\\\\$1");
    }
}
