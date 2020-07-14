package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public interface SearchResults<E extends RedisSearchableEntity> extends Iterable<SearchResult<String, Object>> {

    Long getTotalResults();

    List<SearchResult<String, Object>> getResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }

    @Override
    default Iterator<SearchResult<String, Object>> iterator() {

        return getResults().iterator();
    }

    default Stream<SearchResult<String, Object>> stream() {

        return getResults().stream();
    }
}