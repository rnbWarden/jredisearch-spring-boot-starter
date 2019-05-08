package com.rnbwarden.redisearch.redis.entity;

import com.redislabs.lettusearch.search.field.Field;
import io.redisearch.Schema;

import java.util.function.Function;

public abstract class SearchableField<E> {

    private final String name;
    private final Schema.Field jettisField;
    private final com.redislabs.lettusearch.search.field.Field lettuceField;
    private final Function<E, Object> serializeFunction;
    private final String querySyntax;

    SearchableField(Schema.Field jettisField,
                    com.redislabs.lettusearch.search.field.Field lettuceField,
                    Function<E, Object> serializeFunction,
                    String querySyntax) {

        this.name = jettisField.name;
        this.jettisField = jettisField;
        this.lettuceField = lettuceField;
        this.serializeFunction = serializeFunction;
        this.querySyntax = querySyntax;
    }

    public String getName() {

        return name;
    }

    public Schema.Field getJettisField() {

        return jettisField;
    }

    public Field getLettuceField() {

        return lettuceField;
    }

    public Object serialize(E entity) {

        return serializeFunction.apply(entity);
    }

    public String getQuerySyntax(String value) {

        return String.format(querySyntax, value);
    }
}