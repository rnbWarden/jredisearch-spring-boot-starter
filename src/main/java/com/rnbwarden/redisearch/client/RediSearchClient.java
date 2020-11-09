package com.rnbwarden.redisearch.client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;

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

    default SearchContext<E> getSearchContextWithFields(Map<String, String> fieldNameValues) {

        return getSearchContextWithFields(fieldNameValues, null);
    }

    default SearchContext<E> getSearchContextWithFields(String fieldName, Collection<String> fieldValues) {

        return getSearchContextWithFields(null, Map.of(fieldName, fieldValues));
    }

    SearchContext<E> getSearchContextWithFields(Map<String, String> fieldNameValues,
                                                Map<String, Collection<String>> multiValuedFieldsNameValues);

    List<E> findByKeys(Collection<String> keys);

    SearchResults<E> find(SearchContext<E> searchContext);


    default PageableSearchResults<E> searchByFields(Map<String, String> fieldNameValues) {

        return search(getPagingSearchContextWithFields(fieldNameValues));
    }

    default PagingSearchContext<E> getPagingSearchContextWithFields(Map<String, String> fieldNameValues) {

        return getPagingSearchContextWithFields(fieldNameValues, null);
    }

    PagingSearchContext<E> getPagingSearchContextWithFields(Map<String, String> fieldNameValues,
                                                            Map<String, Collection<String>> multiValuedFieldsNameValues);

    PageableSearchResults<E> search(PagingSearchContext<E> pagingSearchContext);

    PageableSearchResults<E> findAll();
    PageableSearchResults<E> findAll(Integer limit);
    PageableSearchResults<E> findAll(PagingSearchContext<E> pagingSearchContext);

    List<E> deserialize(SearchResults<E> searchResults);
}