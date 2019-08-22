package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RediSearchClient<E extends RedisSearchableEntity> {

    void recreateIndex();
    void dropIndex();
    Long getKeyCount();

    void save(E entity);
    void delete(String key);

    Optional<E> findByKey(String key);

    default SearchResults<E> findByFields(Map<String, String> fieldNameValues) {

        return find(getSearchContextWithFields(fieldNameValues));
    }

    SearchContext getSearchContextWithFields(Map<String, String> fieldNameValues);

    List<E> findByKeys(Collection<String> keys);

    SearchResults<E> find(SearchContext searchContext);

    default PageableSearchResults<E> searchByFields(Map<String, String> fieldNameValues) {

        return search(getPagingSearchContextWithFields(fieldNameValues));
    }

    PagingSearchContext getPagingSearchContextWithFields(Map<String, String> fieldNameValues);

    PageableSearchResults<E> search(PagingSearchContext pagingSearchContext);

    PageableSearchResults<E> findAll(Integer limit);

    PageableSearchResults<E> findAll(PagingSearchContext pagingSearchContext);

    List<E> deserialize(SearchResults<E> searchResults);
}