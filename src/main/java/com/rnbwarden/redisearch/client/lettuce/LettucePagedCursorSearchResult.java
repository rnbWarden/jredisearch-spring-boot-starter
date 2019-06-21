package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class LettucePagedCursorSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final E entity;

    LettucePagedCursorSearchResult(Map<String, Object> fields, LettuceRediSearchClient<E> lettuceRediSearchClient) {

        this.entity = lettuceRediSearchClient.deserialize(fields);
    }

    public String getKey() {

        return ofNullable(entity)
                .map(RedisSearchableEntity::getPersistenceKey)
                .orElse(null);
    }

    public Optional<E> getResult() {

        return ofNullable(entity);
    }
}