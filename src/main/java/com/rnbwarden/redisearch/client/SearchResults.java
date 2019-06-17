package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.List;

public interface SearchResults<E extends RedisSearchableEntity> {

    Long getTotalResults();

    List<SearchResult<String, Object>> getResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }
}