package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;

public class LettucePagedSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final String key;
    private final LettuceRediSearchClient<E> lettuceRediSearchClient;

    LettucePagedSearchResult(String key, LettuceRediSearchClient<E> lettuceRediSearchClient) {

        this.key = key;
        this.lettuceRediSearchClient = lettuceRediSearchClient;
    }

    public String getKey() {

        return key;
    }

    public Optional<E> getResult() {

        return lettuceRediSearchClient.findByQualifiedKey(key);
    }
}