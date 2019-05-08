package com.rnbwarden.redisearch.redis.entity;

import com.redislabs.lettusearch.search.field.TagField;
import io.redisearch.Schema;

import java.util.function.Function;

public class SearchableTagField<E> extends SearchableField<E> {

    private static final String QUERY_SYNTAX = "{%s}";

    public SearchableTagField(String name,
                              Function<E, Object> serializeFunction) {

        super(new Schema.TextField(name),
                TagField.builder().name(name).build(),
                serializeFunction,
                QUERY_SYNTAX);
    }
}