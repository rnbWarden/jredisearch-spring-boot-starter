package com.rnbwarden.redisearch.redis.entity;

import com.redislabs.lettusearch.search.field.TextField;
import io.redisearch.Schema;

import java.util.function.Function;

public class SearchableTextField<E> extends SearchableField<E> {

    private static final String QUERY_SYNTAX = "%s";

    public SearchableTextField(String name,
                               Function<E, Object> serializeFunction) {

        super(new Schema.TextField(name),
                TextField.builder().name(name).build(),
                serializeFunction,
                QUERY_SYNTAX);
    }
}
