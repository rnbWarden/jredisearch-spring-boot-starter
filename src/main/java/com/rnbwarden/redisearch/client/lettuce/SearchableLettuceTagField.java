package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.index.field.TagField;
import com.rnbwarden.redisearch.client.SearchableTagField;

import java.util.function.Function;

public class SearchableLettuceTagField<E> extends SearchableLettuceField<E> implements SearchableTagField {

    public SearchableLettuceTagField(String name,
                                     boolean sortable,
                                     Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, TagField.builder().name(name).sortable(sortable).build());
    }
}
