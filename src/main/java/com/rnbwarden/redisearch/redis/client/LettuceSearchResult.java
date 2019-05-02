package com.rnbwarden.redisearch.redis.client;

import com.redislabs.lettusearch.search.SearchResults;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class LettuceSearchResult implements SearchResult {

    private final SearchResults delegate;

    LettuceSearchResult(SearchResults delegate) {

        this.delegate = delegate;
    }

    @Override
    public Long getTotalResults() {

        return delegate.getCount();
    }

    @Override
    public List<Object> getFieldsByKey(String key) {

        List<com.redislabs.lettusearch.search.SearchResult> results = delegate.getResults();
        return results.stream()
                .filter(Objects::nonNull)
                .map(com.redislabs.lettusearch.search.SearchResult::getFields)
                .filter(Objects::nonNull)
                .map(fields -> fields.get(key))
                .collect(toList());

    }
}
