package com.rnbwarden.redisearch.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@RediSearchEntity(name = "stub")
public class StubEntity implements RedisSearchableEntity {

    private String key;

    @RediSearchField(name = "column1")
    String column1;

    @Override //@JsonIgnore
    public String getPersistenceKey() {

        return key;
    }
}
