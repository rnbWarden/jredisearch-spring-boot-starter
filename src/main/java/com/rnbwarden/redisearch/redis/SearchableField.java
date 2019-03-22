package com.rnbwarden.redisearch.redis;

import com.redislabs.lettusearch.search.field.Field;
import io.redisearch.Schema;

import java.util.function.Function;

public abstract class SearchableField<E> {

    private final String name;
    private final Schema.Field jettisField;
    private final com.redislabs.lettusearch.search.field.Field lettuceField;
    private final Function<E, Object> serializeFunction;

    public SearchableField(Schema.Field jettisField,
                           com.redislabs.lettusearch.search.field.Field lettuceField,
                           Function<E, Object> serializeFunction) {

        this.name = jettisField.name;
        this.jettisField = jettisField;
        this.lettuceField = lettuceField;
        this.serializeFunction = serializeFunction;
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

        return "%s";
    }
}