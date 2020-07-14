package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.entity.SearchableField;
import io.redisearch.Schema;

import java.util.function.Function;

public abstract class SearchableJedisField<E> extends SearchableField<E> {

    protected final Schema.Field field;
    protected boolean isSearchable = true;

    SearchableJedisField(String name,
                         Function<E, String> serializeFunction,
                         String querySyntax,
                         Schema.Field field) {

        super(name, serializeFunction, querySyntax, field.sortable);
        this.field = field;
    }

    Schema.Field getField() {

        return field;
    }

    boolean isSearchable() {

        return isSearchable;
    }
}