package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;
import java.util.function.Consumer;

public class LettucePagedSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final String key;
    private final LettuceRediSearchClient<E> lettuceRediSearchClient;
    private final Consumer<Exception> exceptionConsumer;

    LettucePagedSearchResult(String keyPrefix,
                             String key,
                             LettuceRediSearchClient<E> lettuceRediSearchClient,
                             Consumer<Exception> exceptionConsumer) {

        this.key = key.substring(keyPrefix.length());
        this.lettuceRediSearchClient = lettuceRediSearchClient;
        this.exceptionConsumer = exceptionConsumer;
    }

    public String getKey() {

        return key;
    }

    public Optional<E> getResult() {

        try {
            return lettuceRediSearchClient.findByKey(key);
        } catch (Exception e) {
            if (exceptionConsumer != null) {
                exceptionConsumer.accept(e);
            }
            return Optional.empty();
        }
    }
}