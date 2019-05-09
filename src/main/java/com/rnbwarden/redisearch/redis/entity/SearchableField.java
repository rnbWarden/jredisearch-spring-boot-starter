package com.rnbwarden.redisearch.redis.entity;

import java.util.function.Function;

public abstract class SearchableField<E> {

    protected final String name;
    protected final Function<E, Object> serializeFunction;
    protected final String querySyntax;

    public SearchableField(String name,
                           Function<E, Object> serializeFunction,
                           String querySyntax) {

        this.name = name;
        this.serializeFunction = serializeFunction;
        this.querySyntax = querySyntax;
    }

    public String getName() {

        return name;
    }

    public Object serialize(E entity) {

        return serializeFunction.apply(entity);
    }

    public String getQuerySyntax(String value) {

        return String.format(querySyntax, value);
    }
}