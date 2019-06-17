package com.rnbwarden.redisearch.entity;

import java.io.Serializable;
import java.math.BigDecimal;

@RediSearchEntity(name = "testskupriceidx2")
public class CurrentPrice implements Serializable, RedisSearchableEntity {

    @RediSearchField(name = "skuId")
    String sku;

    @RediSearchField(name = "originalSku")
    String originalSku;

    String source;

    @RediSearchField(name = "productId")
    String product;

    @RediSearchField(name = "originalProduct")
    String originalProduct;

    BigDecimal price;

    @RediSearchField(name = "brand")
    String brand;
    String storeId;
    Long version;

    @Override
    public String getPersistenceKey() {

        return sku;
    }
}