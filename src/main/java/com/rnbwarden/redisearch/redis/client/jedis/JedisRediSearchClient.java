package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.CompressingJacksonSerializer;
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

    public JedisRediSearchClient(Client jRediSearchClient,
                                 CompressingJacksonSerializer<E> redisSerializer,
                                 Long defaultMaxResults) {

        super(redisSerializer, defaultMaxResults);
        this.jRediSearchClient = jRediSearchClient;
        checkAndCreateIndex();
    }

    @Override
    protected Map<RediSearchFieldType, BiFunction<String, Function<E, Object>, SearchableJedisField<E>>> getFieldStrategy() {

        Map<RediSearchFieldType, BiFunction<String, Function<E, Object>, SearchableJedisField<E>>> fieldStrategy = new HashMap<>();
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
        jRediSearchClient.addDocument(entity.getPersistenceKey(), 1, fields, false, true, null);
    }

    @Override
    public void delete(String key) {

        jRediSearchClient.deleteDocument(key, true);
    }

    @Override
    public Optional<E> findByKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(jRediSearchClient.getDocument(key, false))
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

        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            query.limit(rediSearchOptions.getOffset().intValue(), rediSearchOptions.getLimit().intValue());
        }
        return query;
    }

    @Override
    protected SearchResults search(String queryString, RediSearchOptions rediSearchOptions) {

        Query query = new Query(queryString);
        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            query.limit(rediSearchOptions.getOffset().intValue(), rediSearchOptions.getLimit().intValue());
        }
        if (rediSearchOptions.isNoContent()) {
            query.setNoContent();
        }
        return search(query);
    }

    private SearchResults search(Query query) {

        io.redisearch.SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());

        return new JedisSearchResults(searchResult);
    }
}