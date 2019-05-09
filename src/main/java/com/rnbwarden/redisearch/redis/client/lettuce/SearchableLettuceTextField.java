package com.rnbwarden.redisearch.redis.client.lettuce;

import com.redislabs.lettusearch.search.field.TextField;
import com.rnbwarden.redisearch.redis.client.SearchableTextField;

import java.util.function.Function;

public class SearchableLettuceTextField<E> extends SearchableLettuceField<E> implements SearchableTextField {

    public SearchableLettuceTextField(String name,
                                      Function<E, Object> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, TextField.builder().name(name).build());
    }
}
