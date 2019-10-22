package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;
import java.util.function.Consumer;

public class JedisPagedSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final String key;
    private final JedisRediSearchClient<E> jedisRediSearchClient;
    private final Consumer<Exception> exceptionHandler;

    public JedisPagedSearchResult(String key,
                                  JedisRediSearchClient<E> jedisRediSearchClient,
                                  Consumer<Exception> exceptionHandler) {

        this.key = key;
        this.jedisRediSearchClient = jedisRediSearchClient;
        this.exceptionHandler = exceptionHandler;
    }

    public String getKey() {

        return key;
    }

    public Optional<E> getResult() {

        try {
            return jedisRediSearchClient.findByQualifiedKey(key);
        } catch (Exception e) {
            if (exceptionHandler != null) {
                exceptionHandler.accept(e);
            }
            return Optional.empty();
        }
    }
}