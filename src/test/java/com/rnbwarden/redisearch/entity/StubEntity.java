package com.rnbwarden.redisearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@RediSearchEntity(name = "stub")
public class StubEntity implements RedisSearchableEntity {

    public static final String COLUMN1 = "column1";
    public static final String LIST_COLUMN = "listColumn";
    private String key;
    @RediSearchField(name = COLUMN1)
    String column1;
    @RediSearchField(name = LIST_COLUMN)
    private List<String> multiValuedField;

    @Override //@JsonIgnore
    public String getPersistenceKey() {

        return key;
    }
}
