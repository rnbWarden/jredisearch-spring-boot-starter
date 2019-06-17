package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.*;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.querybuilder.QueryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static io.redisearch.querybuilder.QueryBuilder.intersect;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

public class JedisRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableJedisField<E>> {

    private static final Logger logger = LoggerFactory.getLogger(JedisRediSearchClient.class);
    private final Client jRediSearchClient;

    public JedisRediSearchClient(Class<E> clazz,
                                 Client jRediSearchClient,
                                 RedisSerializer<E> redisSerializer,
                                 Long defaultMaxResults) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.jRediSearchClient = jRediSearchClient;
        checkAndCreateIndex();
    }

    @SuppressWarnings("unchecked")
    protected SearchableJedisField<E> createSearchableField(RediSearchFieldType type,
                                                            String name,
                                                            boolean sortable,
                                                            Function<E, String> serializationFunction) {

        if (type == RediSearchFieldType.TEXT) {
            return new SearchableJedisTextField(name, sortable, serializationFunction);
        }
        if (type == RediSearchFieldType.TAG) {
            return new SearchableJedisTagField(name, sortable, serializationFunction);
        }
        throw new IllegalArgumentException(format("field type '%s' is not supported", type));
    }

    @Override
    protected void checkAndCreateIndex() {

        try {
            jRediSearchClient.getInfo();
        } catch (JedisDataException jde) {
            this.jRediSearchClient.createIndex(createSchema(), Client.IndexOptions.defaultOptions());
        }
    }

    private Schema createSchema() {

        Schema schema = new Schema();
        getFields().stream()
                .map(SearchableJedisField::getField)
                .forEach(schema::addField);
        return schema;
    }

    @Override
    public void dropIndex() {

        jRediSearchClient.dropIndex(true);
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        jRediSearchClient.addDocument(key, 1, fields, false, true, null);
    }

    @Override
    public void delete(String key) {

        jRediSearchClient.deleteDocument(getQualifiedKey(key), true);
    }

    @Override
    public Optional<E> findByKey(String key) {

        return findByQualifiedKey(getQualifiedKey(key));
    }

    Optional<E> findByQualifiedKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(jRediSearchClient.getDocument(key, false))
                        .map(d -> d.get(SERIALIZED_DOCUMENT))
                        .map(b -> (byte[]) b)
                        .map(redisSerializer::deserialize)
        );
    }

    @Override
    public SearchResults find(SearchContext context) {

        return performTimedOperation("search", () -> search(buildQuery(context)));
    }

    private Query buildQuery(SearchContext searchContext) {

        QueryNode node = intersect();
        searchContext.getQueryFields().forEach(queryField -> node.add(queryField.getName(), queryField.getQuerySyntax()));
        Query query = new Query(node.toString());

        configureQueryOptions(searchContext, query);
        return query;
    }

    private void configureQueryOptions(SearchContext searchContext, Query query) {

        if (searchContext.isNoContent()) {
            query.setNoContent();
        }
        ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> query.setSortBy(sortBy, searchContext.isSortAscending()));
    }

    @Override
    protected SearchResults<E> search(String queryString, SearchContext searchContext) {

        Query query = new Query(queryString);
        configureQueryOptions(searchContext, query);
        return search(query);
    }

    @SuppressWarnings("unchecked")
    private SearchResults<E> search(Query query) {

        return new JedisSearchResults(keyPrefix, performJedisSearch(query));
    }

    private SearchResult performJedisSearch(Query query) {

        SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());
        return searchResult;
    }

    @Override
    public PageableSearchResults<E> search(PagingSearchContext pageableContent) {

        return performTimedOperation("search", () -> performPagedJedisSearch(buildQuery(pageableContent)));
    }

    @Override
    protected PageableSearchResults<E> search(String queryString, PagingSearchContext searchContext) {

        assert (searchContext != null);
        assert (searchContext.getOffset() != null);
        assert (searchContext.getLimit() != null);

        //aggregateSearch(queryString, searchContext);

        return performTimedOperation("search", () -> {
            Query query = new Query(queryString);
            configureQueryOptions(searchContext, query);
            return performPagedJedisSearch(query);
        });
    }

    private JedisPagingSearchResults<E> performPagedJedisSearch(Query query) {

        return new JedisPagingSearchResults<>(performJedisSearch(query), this);
    }

/**
 private void aggregateSearch(String queryString, PagingSearchContext searchContext) {

 AggregationRequest aggregationRequest = new AggregationRequest(queryString)
 .limit(searchContext.getLimit().intValue());

 ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> {
 SortedField sortedField = new SortedField(sortBy, searchContext.isSortAscending() ? SortedField.SortOrder.ASC : SortedField.SortOrder.DESC);
 aggregationRequest.sortBy(sortedField);
 });

 AggregationResult aggregationResult = jRediSearchClient.aggregate(aggregationRequest);
 }
 */
}