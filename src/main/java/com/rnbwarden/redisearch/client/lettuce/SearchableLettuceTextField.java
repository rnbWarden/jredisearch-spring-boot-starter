package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.search.field.TextField;
import com.rnbwarden.redisearch.client.SearchableTextField;

import java.util.function.Function;

public class SearchableLettuceTextField<E> extends SearchableLettuceField<E> implements SearchableTextField {

    public SearchableLettuceTextField(String name,
                                      boolean sortable,
                                      Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, TextField.builder().name(name).sortable(sortable).build());
    }
}
