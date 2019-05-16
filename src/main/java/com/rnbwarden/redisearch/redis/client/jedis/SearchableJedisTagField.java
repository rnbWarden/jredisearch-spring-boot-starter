package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.SearchableTagField;
import io.redisearch.Schema;

import java.util.function.Function;

public class SearchableJedisTagField<E> extends SearchableJedisField<E> implements SearchableTagField {

    public SearchableJedisTagField(String name,
                                   Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, new Schema.TagField(name));
    }
}
