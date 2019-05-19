package com.rnbwarden.redisearch.client;

import java.util.Map;

public interface SearchResult<K, V> {

    Map<K, V> getFields();
    V getField(K key);

    K getId();
}
