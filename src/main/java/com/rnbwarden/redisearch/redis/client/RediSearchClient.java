package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.redis.client.options.RediSearchOptions;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.Optional;

public interface RediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity, S extends RediSearchOptions> {

    void recreateIndex();
    void dropIndex();
    Long getKeyCount();

    void save(E entity);
    void delete(String key);

    SearchResult findAll(Integer offset, Integer limit, boolean includeContent);
    Optional<E> findByKey(String key);
    SearchResult findByFields(Map<String, String> fieldNameValues, Long offset, Long limit);
    SearchResult findByFields(Map<String, String> fieldNameValues, S options);
    SearchResult find(S options);
}
