package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RediSearchClient<E extends RedisSearchableEntity> {

    void recreateIndex();
    void dropIndex();
    Long getKeyCount();

    void save(E entity);
    void delete(String key);

    SearchableField<E> getField(String name);

    SearchResults findAll(@NonNull Integer offset, @NonNull Integer limit, @NonNull boolean includeContent);
    Optional<E> findByKey(String key);
    default SearchResults findByFields(Map<String, String> fieldNameValues) {

        return findByFields(fieldNameValues, null, null);
    }
    SearchResults findByFields(Map<String, String> fieldNameValues, Long offset, Long limit);
    SearchResults findByFields(Map<String, String> fieldNameValues, RediSearchOptions options);
    SearchResults find(RediSearchOptions options);

    List<E> deserialize(SearchResults searchResults);
}