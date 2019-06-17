package com.rnbwarden.redisearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@RediSearchEntity(name = "sku")
public class StubSkuEntity implements RedisSearchableEntity {

    public static final String KEY = "key";
    public static final String BRAND = "brand";

    @RediSearchField(name = KEY, sortable = true)
    String key;

    @RediSearchField(name = BRAND)
    String brand;


    @Override //@JsonIgnore
    public String getPersistenceKey() {

        return key;
    }
}
