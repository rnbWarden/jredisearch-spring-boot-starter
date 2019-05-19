package com.rnbwarden.redisearch.entity;

import java.util.function.Function;

public abstract class SearchableField<E> {

    protected final String name;
    protected final Function<E, String> serializeFunction;
    protected final String querySyntax;

    public SearchableField(String name,
                           Function<E, String> serializeFunction,
                           String querySyntax) {

        this.name = name;
        this.serializeFunction = serializeFunction;
        this.querySyntax = querySyntax;
    }

    public String getName() {

        return name;
    }

    public String serialize(E entity) {

        return serializeFunction.apply(entity);
    }

    public String getQuerySyntax(String value) {

        return String.format(querySyntax, value);
    }
}