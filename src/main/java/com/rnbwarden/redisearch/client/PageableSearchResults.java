package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.stream.Stream;

public interface PageableSearchResults<E extends RedisSearchableEntity> {

    Long getTotalResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }

    default Stream<PagedSearchResult<E>> getResultStream() {

        return getResultStream(false);
    }

    Stream<PagedSearchResult<E>> getResultStream(boolean useParallel);
}