package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.client.RediSearchOptions;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private com.redislabs.lettusearch.RediSearchClient rediSearchClient;
    private final GenericObjectPool<StatefulRediSearchConnection<String, Object>> pool;

    public LettuceRediSearchClient(Class<E> clazz,
                                   com.redislabs.lettusearch.RediSearchClient rediSearchClient,
                                   RedisCodec<String, Object> redisCodec,
                                   RedisSerializer<E> redisSerializer,
                                   Long defaultMaxResults) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.rediSearchClient = rediSearchClient;
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> rediSearchClient.connect(redisCodec), new GenericObjectPoolConfig());
        checkAndCreateIndex();
    }

    @Override
    protected Map<RediSearchFieldType, BiFunction<String, Function<E, String>, SearchableLettuceField<E>>> getFieldStrategy() {

        Map<RediSearchFieldType, BiFunction<String, Function<E, String>, SearchableLettuceField<E>>> fieldStrategy = new HashMap<>();
        fieldStrategy.put(RediSearchFieldType.TEXT, SearchableLettuceTextField::new);
        fieldStrategy.put(RediSearchFieldType.TAG, SearchableLettuceTagField::new);
        return fieldStrategy;
    }

    @Override
    protected void checkAndCreateIndex() {

        StatefulRediSearchConnection<String, String> uncompressedConnection = null;
        try {
            uncompressedConnection = rediSearchClient.connect();
            uncompressedConnection.sync().indexInfo(index);
        } catch (RedisCommandExecutionException ex) {
            if (uncompressedConnection == null) {
                throw ex;
            }
            if (ex.getCause().getMessage().equals("Unknown Index name")) {
                uncompressedConnection.sync().create(index, createSchema());
            }
        } finally {
            if (uncompressedConnection != null) {
                uncompressedConnection.close();
            }
        }
    }

    private Schema createSchema() {

        Schema.SchemaBuilder builder = Schema.builder();
        getFields().stream()
                .map(SearchableLettuceField::getField)
                .forEach(builder::field);
        return builder.build();
    }

    @Override
    public void dropIndex() {

        try (StatefulRediSearchConnection<String, String> uncompressedConnection = rediSearchClient.connect()) {
            uncompressedConnection.sync().drop(index, DropOptions.builder().keepDocs(false).build());
        }
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        execute(connection -> connection.sync().add(index, key, 1, fields, AddOptions.builder().replace(true).build()));
    }

    @Override
    public void delete(String key) {

        execute(connection -> connection.sync().del(index, getQualifiedKey(key), true));
    }

    @Override
    public Optional<E> findByKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(getByKey(getQualifiedKey(key)))
                        .map(map -> map.get(SERIALIZED_DOCUMENT))
                        .map(byte[].class::cast)
                        .map(redisSerializer::deserialize));
    }

    private Map<String, Object> getByKey(String key) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return connection.sync().get(index, key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchResults find(RediSearchOptions options) {

        return performTimedOperation("search", () -> search(buildQueryString(options), options));
    }

    @Override
    protected SearchResults search(String queryString, RediSearchOptions options) {

//        SearchResults searchResults = connection.sync().search(index, query.toString(), SearchOptions.builder().build());
//        SearchOptions.builder().returnField(FIELD_NAME).returnField(FIELD_STYLE).build()
        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, buildSearchOptions(options));
            logger.debug("found count {}", searchResults.getCount());
            return new LettuceSearchResults(keyPrefix, searchResults);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String buildQueryString(RediSearchOptions rediSearchOptions) {

        return rediSearchOptions.getFieldNameValues().entrySet().stream()
                .map(entry -> {
                    SearchableField field = entry.getKey();
                    return format("@%s:%s", field.getName(), field.getQuerySyntax(entry.getValue()));
                })
                .collect(joining(" ")); //space imply intersection - AND
    }

    private SearchOptions buildSearchOptions(RediSearchOptions rediSearchOptions) {

        SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            builder.limit(Limit.builder().num(rediSearchOptions.getLimit()).offset(rediSearchOptions.getOffset()).build());
        }
        builder.noContent(rediSearchOptions.isNoContent());
        return builder.build();
    }

    private void execute(Consumer<StatefulRediSearchConnection<String, Object>> consumer) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            consumer.accept(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}