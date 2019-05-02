package com.rnbwarden.redisearch.redis.client;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class JedisSearchResult implements SearchResult {

    private final io.redisearch.SearchResult delegate;

    JedisSearchResult(io.redisearch.SearchResult delegate) {

        this.delegate = delegate;
    }

    @Override
    public Long getTotalResults() {

        return delegate.totalResults;
    }

    @Override
    public List<Object> getFieldsByKey(String key) {

        return delegate.docs.stream()
                .filter(Objects::nonNull)
                .map(d -> d.get(key))
                .collect(toList());
    }
}
