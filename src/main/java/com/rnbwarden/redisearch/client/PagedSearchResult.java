package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;

public interface PagedSearchResult<E extends RedisSearchableEntity> {

    String getKey();

    Optional<E> getResult();
}