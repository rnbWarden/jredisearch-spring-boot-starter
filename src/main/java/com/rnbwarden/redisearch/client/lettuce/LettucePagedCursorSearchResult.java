package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;

import static java.util.Optional.ofNullable;

public class LettucePagedCursorSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final E entity;

    LettucePagedCursorSearchResult(E entity) {

        this.entity = entity;
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