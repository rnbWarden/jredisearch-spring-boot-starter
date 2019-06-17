package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.SearchResult;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.redisearch.Document;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class JedisSearchResults<E extends RedisSearchableEntity> implements SearchResults<E> {

    private final io.redisearch.SearchResult delegate;
    private final String keyPrefix;

    JedisSearchResults(String keyPrefix, io.redisearch.SearchResult delegate) {

        this.keyPrefix = keyPrefix;
        this.delegate = delegate;
    }

    @Override
    public Long getTotalResults() {

        return delegate.totalResults;
    }

    @Override
    public List<SearchResult<String, Object>> getResults() {

        return delegate.docs.stream()
                .filter(Objects::nonNull)
                .map(this::createSearchResult)
                .map(result -> (SearchResult<String, Object>) result)
                .collect(toList());
    }

    private JedisSearchResult createSearchResult(Document jedisSearchResult) {

        return new JedisSearchResult(keyPrefix, jedisSearchResult);
    }
}