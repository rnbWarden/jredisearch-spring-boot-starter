package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import com.rnbwarden.redisearch.redis.client.SearchResults;
import com.rnbwarden.redisearch.redis.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import io.redisearch.querybuilder.QueryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.redisearch.querybuilder.QueryBuilder.intersect;
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

    @Override
    protected Map<RediSearchFieldType, BiFunction<String, Function<E, String>, SearchableJedisField<E>>> getFieldStrategy() {

        Map<RediSearchFieldType, BiFunction<String, Function<E, String>, SearchableJedisField<E>>> fieldStrategy = new HashMap<>();
        fieldStrategy.put(RediSearchFieldType.TEXT, SearchableJedisTextField::new);
        fieldStrategy.put(RediSearchFieldType.TAG, SearchableJedisTagField::new);
        return fieldStrategy;
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

        return performTimedOperation("findByKey",
                () -> ofNullable(jRediSearchClient.getDocument(getQualifiedKey(key), false))
                        .map(d -> d.get(SERIALIZED_DOCUMENT))
                        .map(b -> (byte[]) b)
                        .map(redisSerializer::deserialize)
        );
    }

    @Override
    public SearchResults find(RediSearchOptions options) {

        return performTimedOperation("search", () -> search(buildQuery(options)));
    }

    private Query buildQuery(RediSearchOptions rediSearchOptions) {

        QueryNode node = intersect();
        rediSearchOptions.getFieldNameValues().forEach((field, value) -> node.add(field.getName(), field.getQuerySyntax(value)));
        Query query = new Query(node.toString());

        configureQueryOptions(rediSearchOptions, query);
        return query;
    }

    private void configureQueryOptions(RediSearchOptions rediSearchOptions, Query query) {

        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            query.limit(rediSearchOptions.getOffset().intValue(), rediSearchOptions.getLimit().intValue());
        } else {
            query.limit(0, 1000000);
        }
        if (rediSearchOptions.isNoContent()) {
            query.setNoContent();
        }
    }

    @Override
    protected SearchResults search(String queryString, RediSearchOptions rediSearchOptions) {

        Query query = new Query(queryString);
        configureQueryOptions(rediSearchOptions, query);
        return search(query);
    }

    private SearchResults search(Query query) {

        io.redisearch.SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());

        return new JedisSearchResults(keyPrefix, searchResult);
    }
}