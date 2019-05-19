package com.rnbwarden.redisearch.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface RedisSearchableEntity {

    @JsonIgnore
    String getPersistenceKey();
}