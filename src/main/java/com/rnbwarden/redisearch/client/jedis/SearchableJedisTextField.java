package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.SearchableTextField;
import io.redisearch.Schema;

import java.util.function.Function;

public class SearchableJedisTextField<E> extends SearchableJedisField<E> implements SearchableTextField {

    public SearchableJedisTextField(String name,
                                    boolean sortable,
                                    Function<E, String> serializeFunction) {

        super(name, serializeFunction, QUERY_SYNTAX, new Schema.TextField(name, 1.0, sortable));
    }
}
