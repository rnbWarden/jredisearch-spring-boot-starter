package com.rnbwarden.redisearch.redis;

import io.redisearch.SearchResult;

import java.util.Optional;

public interface RediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity> {

    void dropIndex();
    void recreateIndex();
    void save(E entity);
    void delete(String key);
    Optional<E> findByKey(String key);
    SearchResult findAll(Integer offset,
                         Integer limit,
                         boolean includeContent);
    Long getKeyCount();
}
