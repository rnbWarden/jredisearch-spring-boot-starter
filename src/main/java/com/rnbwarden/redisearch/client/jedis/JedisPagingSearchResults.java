package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.redisearch.SearchResult;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class JedisPagingSearchResults<E extends RedisSearchableEntity> implements PageableSearchResults<E> {

    private final io.redisearch.SearchResult delegate;
    private final JedisRediSearchClient<E> jedisRediSearchClient;
    private final Consumer<Exception> exceptionHandler;

    JedisPagingSearchResults(SearchResult delegate,
                             JedisRediSearchClient<E> jedisRediSearchClient,
                             Consumer<Exception> exceptionHandler) {

        this.delegate = delegate;
        this.jedisRediSearchClient = jedisRediSearchClient;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public Long getTotalResults() {

        return delegate.totalResults;
    }

    @Override
    public Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return (useParallel ? delegate.docs.parallelStream() : delegate.docs.stream())
                .filter(Objects::nonNull)
                .map(this::createSearchResult);
    }

    private PagedSearchResult<E> createSearchResult(io.redisearch.Document document) {

        return new JedisPagedSearchResult<>(document.getId(), jedisRediSearchClient, exceptionHandler);
    }
}
