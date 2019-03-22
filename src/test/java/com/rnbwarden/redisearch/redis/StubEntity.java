package com.rnbwarden.redisearch.redis;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@RediSearchEntity(name = "stub")
public class StubEntity implements RedisSearchableEntity {

    private String key;

    @Override
    public String getPersistenceKey() {

        return key;
    }
}
