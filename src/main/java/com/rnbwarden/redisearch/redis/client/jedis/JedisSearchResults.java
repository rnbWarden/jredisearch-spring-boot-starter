package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.SearchResult;
import com.rnbwarden.redisearch.redis.client.SearchResults;
import io.redisearch.Document;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class JedisSearchResults implements SearchResults {

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