package com.rnbwarden.redisearch.client;

import java.util.Map;

public interface SearchResult<K, V> {

    Map<K, V> getFields();
    V getField(K key);
    K getId();

    default String getFieldValue(K key) {

        byte[] value = (byte[]) getField(key);
        return value == null ? null : new String(value);
    }
}