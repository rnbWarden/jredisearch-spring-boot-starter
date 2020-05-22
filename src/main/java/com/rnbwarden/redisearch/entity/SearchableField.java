package com.rnbwarden.redisearch.entity;

import java.util.function.Function;

public abstract class SearchableField<E> {

    protected final String name;
    protected final Function<E, String> serializeFunction;
    protected final String querySyntax;
    protected final boolean isSortable;

    public SearchableField(String name,
                           Function<E, String> serializeFunction,
                           String querySyntax,
                           boolean isSortable) {

        this.name = name;
        this.serializeFunction = serializeFunction;
        this.querySyntax = querySyntax;
        this.isSortable = isSortable;
    }

    public String getName() {

        return name;
    }

    public String serialize(E entity) {

        return serializeFunction.apply(entity);
    }

    public String getQuerySyntax(String value) {

        return String.format(querySyntax, escapeSpecialCharacters(value));
    }

    private static final String redisSpecialCharactersRegEx = "([,.<>{}\\[\\]\"':;!@#$%^&*()\\-+=~\\\\]|[&|]{2})";
    public static String escapeSpecialCharacters(String s) {

        return s.replaceAll(redisSpecialCharactersRegEx, "\\\\$1");
    }
}