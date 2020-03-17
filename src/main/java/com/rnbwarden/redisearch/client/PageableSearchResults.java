package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.stream.Stream;

public interface PageableSearchResults<E extends RedisSearchableEntity> {

    Long getTotalResults();

    default boolean hasResults() {

        return getTotalResults() > 0;
    }

    @Deprecated
    default Stream<PagedSearchResult<E>> getResultStream() {

        return parallelStream();
    }

    @Deprecated
    default Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return useParallel ? parallelStream() : resultStream();
    }

    Stream<PagedSearchResult<E>> resultStream();

    Stream<PagedSearchResult<E>> parallelStream();
}