package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.Optional;

public interface RediSearchClient<E extends RedisSearchableEntity, S extends RediSearchOptions> {

    void recreateIndex();
    void dropIndex();
    Long getKeyCount();

    void save(E entity);
    void delete(String key);

    SearchResults findAll(@NonNull Integer offset, @NonNull Integer limit, @NonNull boolean includeContent);

    Optional<E> findByKey(String key);

    default SearchResults findByFields(Map<String, String> fieldNameValues) {

        return findByFields(fieldNameValues, null, null);
    }
    SearchResults findByFields(Map<String, String> fieldNameValues, Long offset, Long limit);
    SearchResults findByFields(Map<String, String> fieldNameValues, S options);

    SearchResults find(S options);
}
