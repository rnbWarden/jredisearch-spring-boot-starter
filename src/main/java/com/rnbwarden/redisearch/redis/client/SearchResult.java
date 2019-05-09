package com.rnbwarden.redisearch.redis.client;

import java.util.Map;

public interface SearchResult<K, V> {

    Map<K, V> getFields();
    V getField(K key);
}
