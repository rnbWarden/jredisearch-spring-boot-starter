package com.rnbwarden.redisearch.autoconfiguration;

import com.rnbwarden.redisearch.redis.entity.RediSearchEntity;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
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
