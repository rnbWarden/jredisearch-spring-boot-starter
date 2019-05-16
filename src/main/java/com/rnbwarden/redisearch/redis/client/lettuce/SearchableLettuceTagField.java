package com.rnbwarden.redisearch.redis.client.lettuce;

import com.redislabs.lettusearch.search.field.TagField;
import com.rnbwarden.redisearch.redis.client.SearchableTagField;

import java.util.function.Function;

public class SearchableLettuceTagField<E> extends SearchableLettuceField<E> implements SearchableTagField {

    public SearchableLettuceTagField(String name,
                                     Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, TagField.builder().name(name).build());
    }
}
