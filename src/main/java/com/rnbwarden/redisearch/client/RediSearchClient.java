package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RediSearchClient<E extends RedisSearchableEntity> {

    Class<E> getType();

    SearchableField<E> getField(String name);

    void recreateIndex();

    void dropIndex();

    Long getKeyCount();

    Long getKeyCount(PagingSearchContext<E> pagingSearchContext);

    void save(E entity);

    void delete(String key);

    Optional<E> findByKey(String key);

    default SearchResults<E> findByFields(Map<String, String> fieldNameValues) {

        return find(getSearchContextWithFields(fieldNameValues));
    }

    SearchContext<E> getSearchContextWithFields(Map<String, String> fieldNameValues);

    SearchContext<E> getSearchContextWithFields(String fieldName, Collection<String> fieldValues);

    List<E> findByKeys(Collection<String> keys);

    SearchResults<E> find(SearchContext<E> searchContext);

    default PageableSearchResults<E> searchByFields(Map<String, String> fieldNameValues) {

        return search(getPagingSearchContextWithFields(fieldNameValues));
    }

    PagingSearchContext<E> getPagingSearchContextWithFields(Map<String, String> fieldNameValues);

    PageableSearchResults<E> search(PagingSearchContext<E> pagingSearchContext);

    PageableSearchResults<E> findAll(Integer limit);

    PageableSearchResults<E> findAll(PagingSearchContext<E> pagingSearchContext);

    List<E> deserialize(SearchResults<E> searchResults);
}