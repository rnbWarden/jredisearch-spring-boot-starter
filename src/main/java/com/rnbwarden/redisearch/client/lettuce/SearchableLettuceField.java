package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.index.field.Field;
import com.rnbwarden.redisearch.entity.SearchableField;

import java.util.function.Function;

public abstract class SearchableLettuceField<E> extends SearchableField<E> {

    private final Field field;

    SearchableLettuceField(String name,
                           Function<E, String> serializeFunction,
                           String querySyntax,
                           Field field) {

        super(name, serializeFunction, querySyntax, field.isSortable());
        this.field = field;
    }

    Field getField() {

        return field;
    }
}