package com.rnbwarden.redisearch.client.jedis;

import java.util.function.Function;

public class NonSearchableJedisField<E> extends SearchableJedisTextField<E> {

    public NonSearchableJedisField(String name,
                                   boolean isSortable,
                                   Function<E, String> serializeFunction) {

        super(name, isSortable, serializeFunction);
    }
}