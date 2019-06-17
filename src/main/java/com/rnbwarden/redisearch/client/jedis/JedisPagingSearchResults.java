package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Objects;
import java.util.stream.Stream;

public class JedisPagingSearchResults<E extends RedisSearchableEntity> implements PageableSearchResults<E> {

    private final io.redisearch.SearchResult delegate;
    private final JedisRediSearchClient<E> jedisRediSearchClient;

    JedisPagingSearchResults(io.redisearch.SearchResult delegate,
                             JedisRediSearchClient<E> jedisRediSearchClient) {

        this.delegate = delegate;
        this.jedisRediSearchClient = jedisRediSearchClient;
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

        return new JedisPagedSearchResult<>(document.getId(), jedisRediSearchClient);
    }
}
