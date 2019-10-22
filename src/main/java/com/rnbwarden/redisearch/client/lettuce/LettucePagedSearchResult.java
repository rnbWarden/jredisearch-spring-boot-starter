package com.rnbwarden.redisearch.client.lettuce;

import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Optional;
import java.util.function.Consumer;

public class LettucePagedSearchResult<E extends RedisSearchableEntity> implements PagedSearchResult<E> {

    private final String key;
    private final LettuceRediSearchClient<E> lettuceRediSearchClient;
    private final Consumer<Exception> exceptionConsumer;

    LettucePagedSearchResult(String key,
                             LettuceRediSearchClient<E> lettuceRediSearchClient,
                             Consumer<Exception> exceptionConsumer) {

        this.key = key;
        this.lettuceRediSearchClient = lettuceRediSearchClient;
        this.exceptionConsumer = exceptionConsumer;
    }

    public String getKey() {

        return key;
    }

    public Optional<E> getResult() {

        try {
            return lettuceRediSearchClient.findByQualifiedKey(key);
        } catch (Exception e) {
            exceptionConsumer.accept(e);
            return Optional.empty();
        }
    }
}