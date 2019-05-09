package com.rnbwarden.redisearch.redis.client.lettuce;

import com.rnbwarden.redisearch.redis.client.SearchResult;
import com.rnbwarden.redisearch.redis.client.SearchResults;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class LettuceSearchResults implements SearchResults<String, Object> {

    private final com.redislabs.lettusearch.search.SearchResults<String, Object> delegate;

    LettuceSearchResults(com.redislabs.lettusearch.search.SearchResults<String, Object> delegate) {

        this.delegate = delegate;
    }

    @Override
    public Long getTotalResults() {

        return delegate.getCount();
    }

    @Override
    public List<SearchResult<String, Object>> getResults() {

        return delegate.getResults().stream()
                .filter(Objects::nonNull)
                .map(LettuceSearchResult::new)
                .collect(toList());
    }
}
