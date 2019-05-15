package com.rnbwarden.redisearch.redis.client.lettuce;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import com.rnbwarden.redisearch.redis.client.SearchResults;
import com.rnbwarden.redisearch.redis.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
import io.lettuce.core.RedisCommandExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final StatefulRediSearchConnection<String, Object> connection;
    private final String index;

    public LettuceRediSearchClient(StatefulRediSearchConnection<String, Object> connection,
                                   CompressingJacksonSerializer<E> redisSerializer,
                                   Long defaultMaxResults) {

        super(redisSerializer, defaultMaxResults);
        this.connection = connection;
        this.index = getIndex(clazz);
        checkAndCreateIndex();
    }

    @Override
    protected Map<RediSearchFieldType, BiFunction<String, Function<E, Object>, SearchableLettuceField<E>>> getFieldStrategy() {

        Map<RediSearchFieldType, BiFunction<String, Function<E, Object>, SearchableLettuceField<E>>> fieldStrategy = new HashMap<>();
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
        connection.sync().add(index, entity.getPersistenceKey(), 1, fields, AddOptions.builder().build());
    }

    @Override
    public void delete(String key) {

        connection.sync().del(key);
    }

    @Override
    public Optional<E> findByKey(String key) {

        SearchOptions searchOptions = SearchOptions.builder().noContent(true).build();

        return performTimedOperation("findByKey",
                () -> ofNullable(connection.sync().search(index, key, searchOptions))
                        .map(com.redislabs.lettusearch.search.SearchResults::getResults)
                        .flatMap(results -> results.stream().findAny())
                        .map(com.redislabs.lettusearch.search.SearchResult::getFields)
                        .map(fields -> (byte[]) fields.get(SERIALIZED_DOCUMENT))
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

        return new LettuceSearchResults(searchResults);
    }

    private String buildQueryString(RediSearchOptions rediSearchOptions) {

        return rediSearchOptions.getFieldNameValues().entrySet().stream()
                .map(entry -> {
                    SearchableField field = entry.getKey();
                    return String.format("@%s:%s", field.getName(), field.getQuerySyntax(entry.getValue()));
                })
                .collect(joining(" ")); //space imply intersection - AND
    }

    private SearchOptions buildSearchOptions(RediSearchOptions rediSearchOptions) {

        SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
        if (rediSearchOptions.getOffset() != null && rediSearchOptions.getLimit() != null) {
            builder.limit(Limit.builder().num(rediSearchOptions.getLimit()).offset(rediSearchOptions.getOffset()).build());
        }
        return builder.build();
    }
}