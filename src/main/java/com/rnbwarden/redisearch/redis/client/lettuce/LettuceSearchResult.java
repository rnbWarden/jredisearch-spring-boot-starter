package com.rnbwarden.redisearch.redis.client.lettuce;

import com.rnbwarden.redisearch.redis.client.SearchResult;

import java.util.Map;

public class LettuceSearchResult<K extends String, V> implements SearchResult<K, V> {

    private final com.redislabs.lettusearch.search.SearchResult<K, V> delegate;
    private final String keyPrefix;

    LettuceSearchResult(String keyPrefix, com.redislabs.lettusearch.search.SearchResult<K, V> searchResult) {

        this.keyPrefix = keyPrefix;
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

    @Override
    @SuppressWarnings("unchecked")
    public K getId() {

        String documentId = delegate.getDocumentId();
        String key = documentId.substring(keyPrefix.length());
        return (K) key;
    }
}