package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;

public class JedisPagedSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final String key;
    private final JedisRediSearchClient<E> jedisRediSearchClient;

    public JedisPagedSearchResult(String key, JedisRediSearchClient<E> jedisRediSearchClient) {

        this.key = key;
        this.jedisRediSearchClient = jedisRediSearchClient;
    }

    public String getKey() {

        return key;
    }

    public Optional<E> getResult() {

        return jedisRediSearchClient.findByQualifiedKey(key);
    }
}