package com.rnbwarden.redisearch.redis.client.lettuce;

import com.rnbwarden.redisearch.redis.client.SearchResult;

import java.util.Map;

public class LettuceSearchResult<K, V> implements SearchResult<K, V> {

    private final com.redislabs.lettusearch.search.SearchResult<K, V> delegate;

     LettuceSearchResult(com.redislabs.lettusearch.search.SearchResult<K, V> searchResult) {

        this.delegate = searchResult;
    }

    @Override
    public Map<K, V> getFields() {

        return delegate.getFields();
    }

    @Override
    public V getField(K key) {

        return delegate.getFields().get(key);
    }
}
