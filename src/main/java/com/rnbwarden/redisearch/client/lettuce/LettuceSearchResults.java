package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.SearchResult;
import com.rnbwarden.redisearch.client.SearchResults;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class LettuceSearchResults implements SearchResults {

    private final com.redislabs.lettusearch.search.SearchResults<String, Object> delegate;
    private final String keyPrefix;

    LettuceSearchResults(String keyPrefix, com.redislabs.lettusearch.search.SearchResults<String, Object> delegate) {

        this.keyPrefix = keyPrefix;
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
                .map(this::createSeachResult)
                .collect(toList());
    }

    private LettuceSearchResult<String, Object> createSeachResult(com.redislabs.lettusearch.search.SearchResult<String, Object> searchResult) {

        return new LettuceSearchResult<>(keyPrefix, searchResult);
    }
}
