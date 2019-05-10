package com.rnbwarden.redisearch.redis.client.lettuce;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import com.rnbwarden.redisearch.redis.client.jedis.SearchableJedisTagField;
import com.rnbwarden.redisearch.redis.client.jedis.SearchableJedisTextField;
import com.rnbwarden.redisearch.redis.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, LettusearchOptions, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final StatefulRediSearchConnection<String, Object> connection;
    private final String index;

    public LettuceRediSearchClient(StatefulRediSearchConnection<String, Object> connection,
                                   CompressingJacksonSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.connection = connection;
        this.index = getIndex(clazz);

        fieldStrategy.put(RediSearchFieldType.TEXT, SearchableLettuceTextField::new);
        fieldStrategy.put(RediSearchFieldType.TAG, SearchableLettuceTagField::new);
    }

    @Override
    public RediSearchOptions getRediSearchOptions() {

        return new LettusearchOptions();
    }

    @Override
    protected void checkAndCreateIndex() {

        try {
            connection.sync().info();
        } catch (JedisDataException jde) {
            connection.sync().create(index, createSchema());
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
    public com.rnbwarden.redisearch.redis.client.SearchResults findAll(Integer offset,
                                                                       Integer limit,
                                                                       boolean includeContent) {

        offset = ofNullable(offset).orElse(0);
        limit = ofNullable(limit).orElse(defaultMaxValue.intValue());

        LettusearchOptions options = (LettusearchOptions) getRediSearchOptions();
        options.setLimit(Long.valueOf(limit));
        options.setOffset(Long.valueOf(offset));
        options.setNoContent(!includeContent);

        return performTimedOperation("findAll", () -> search(ALL_QUERY, options.buildSearchOptions()));
    }

    @Override
    public com.rnbwarden.redisearch.redis.client.SearchResults findByFields(Map<String, String> fieldNameValues,
                                                                            @Nullable Long offset,
                                                                            @Nullable Long limit) {

        LettusearchOptions options = (LettusearchOptions) getRediSearchOptions();
        options.setLimit(limit);
        options.setOffset(offset);
        return findByFields(fieldNameValues, options);
    }

    @Override
    public com.rnbwarden.redisearch.redis.client.SearchResults findByFields(Map<String, String> fieldNameValues,
                                                                            LettusearchOptions options) {

        fieldNameValues.forEach((name, value) -> options.addField(getField(name), value));
        return search(options);
    }

    @Override
    public com.rnbwarden.redisearch.redis.client.SearchResults find(LettusearchOptions options) {

        return performTimedOperation("search", () -> search(options.getQueryString(), options.buildSearchOptions()));
    }

    private LettuceSearchResults search(LettusearchOptions options) {

        return performTimedOperation("search", () -> search(options.buildQueryString(), options.buildSearchOptions()));
    }

    private LettuceSearchResults search(String queryString, com.redislabs.lettusearch.search.SearchOptions searchOptions) {

//        SearchResults searchResults = connection.sync().search(index, query.toString(), SearchOptions.builder().build());
//        SearchOptions.builder().returnField(FIELD_NAME).returnField(FIELD_STYLE).build()

        com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, searchOptions);
        logger.debug("found count {}", searchResults.getCount());

        return new LettuceSearchResults(searchResults);
    }

}