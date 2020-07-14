package com.rnbwarden.redisearch.client.lettuce;

import java.util.function.Function;

public class NonSearchableLettuceField<E> extends SearchableLettuceTextField<E> {

    public NonSearchableLettuceField(String name,
                                     boolean isSortable,
                                     Function<E, String> serializeFunction) {

        super(name, isSortable, serializeFunction);
        this.isSearchable = false;
    }
}