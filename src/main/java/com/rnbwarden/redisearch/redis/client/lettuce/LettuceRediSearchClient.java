package com.rnbwarden.redisearch.redis.client.lettuce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import com.rnbwarden.redisearch.redis.client.SearchResults;
import com.rnbwarden.redisearch.redis.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
import io.lettuce.core.RedisCommandExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final StatefulRediSearchConnection<String, Object> connection;
    private final ObjectMapper objectMapper;

    public LettuceRediSearchClient(Class<E> clazz,
                                   StatefulRediSearchConnection<String, Object> connection,
                                   RedisSerializer<E> redisSerializer,
                                   Long defaultMaxResults,
                                   ObjectMapper objectMapper) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.connection = connection;
        this.objectMapper = objectMapper;
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

        try {
            this.connection.sync().indexInfo(index);
        } catch (RedisCommandExecutionException ex) {
            if (ex.getCause().getMessage().equals("Unknown Index name")) {
                connection.sync().create(index, createSchema());
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

        connection.sync().drop(index, DropOptions.builder().keepDocs(false).build());
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        connection.sync().add(index, key, 1, fields, AddOptions.builder().replace(true).build());
    }

    @Override
    public void delete(String key) {

        connection.sync().del(getQualifiedKey(key));
    }

    @Override
    public Optional<E> findByKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(connection.sync().get(index, getQualifiedKey(key)))
                        .map(byte[].class::cast)
                        .map(redisSerializer::deserialize));
    }

    @Override
    public SearchResults find(RediSearchOptions options) {

        return performTimedOperation("search", () -> search(buildQueryString(options), options));
    }

    @Override
    protected SearchResults search(String queryString, RediSearchOptions options) {

//        SearchResults searchResults = connection.sync().search(index, query.toString(), SearchOptions.builder().build());
//        SearchOptions.builder().returnField(FIELD_NAME).returnField(FIELD_STYLE).build()

        com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, buildSearchOptions(options));
        logger.debug("found count {}", searchResults.getCount());

        return new LettuceSearchResults(keyPrefix, searchResults);
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
}