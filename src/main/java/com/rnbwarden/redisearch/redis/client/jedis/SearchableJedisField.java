package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.entity.SearchableField;
import io.redisearch.Schema;

import java.util.function.Function;

public abstract class SearchableJedisField<E> extends SearchableField<E> {

    private final Schema.Field field;

    SearchableJedisField(String name,
                         Function<E, Object> serializeFunction,
                         String querySyntax,
                         Schema.Field field) {

        super(name, serializeFunction, querySyntax);
        this.field = field;
    }

    public Schema.Field getField() {

        return field;
    }

    public Object serialize(E entity) {

        return serializeFunction.apply(entity);
    }

    public String getQuerySyntax(String value) {

        return String.format(querySyntax, value);
    }
}