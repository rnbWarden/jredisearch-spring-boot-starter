package com.rnbwarden.redisearch.redis.client;

import java.util.List;

public interface SearchResults<K, V> {

    Long getTotalResults();
    List<SearchResult<K, V>> getResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }
}