package com.rnbwarden.redisearch.redis.client;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.client.options.LettusearchOptions;
import com.rnbwarden.redisearch.redis.client.options.RediSearchOptions;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public abstract class LettuceRediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity> extends AbstractRediSearchClient<E, LettusearchOptions> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final StatefulRediSearchConnection<String, Object> connection;
    private final String index;

    public LettuceRediSearchClient(StatefulRediSearchConnection<String, Object> connection,
                                   CompressingJacksonSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.connection = connection;
        this.index = getIndex(clazz);
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
                .map(SearchableField::getLettuceField)
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
                        .map(SearchResults::getResults)
                        .flatMap(results -> results.stream().findAny())
                        .map(com.redislabs.lettusearch.search.SearchResult::getFields)
                        .map(fields -> (byte[]) fields.get(SERIALIZED_DOCUMENT))
                        .map(redisSerializer::deserialize));
    }

    @Override
    public SearchResult findAll(Integer offset,
                                Integer limit,
                                boolean includeContent) {

        offset = ofNullable(offset).orElse(0);
        limit = ofNullable(limit).orElse(defaultMaxValue.intValue());

        LettusearchOptions options = (LettusearchOptions) getRediSearchOptions();
//        options.setQuery(new Query(ALL_QUERY));
        options.setLimit(Long.valueOf(limit));
        options.setOffset(Long.valueOf(offset));
        options.setNoContent(!includeContent);

        return performTimedOperation("findAll", () -> search(options.buildSearchOptions()));
    }

    @Override
    public SearchResult findByFields(Map<String, String> fieldNameValues,
                                     @Nullable Long offset,
                                     @Nullable Long limit) {

        LettusearchOptions options = (LettusearchOptions) getRediSearchOptions();
        options.setLimit(limit);
        options.setOffset(offset);
        return findByFields(fieldNameValues, options);
    }

    @Override
    public SearchResult findByFields(Map<String, String> fieldNameValues,
                                     LettusearchOptions options) {

        fieldNameValues.forEach((name, value) -> options.addField(name, getField(name).getQuerySyntax(value)));
        return performTimedOperation("searchByFields", () -> search(options.buildSearchOptions()));
    }

    @Override
    public SearchResult find(LettusearchOptions options) {

        return performTimedOperation("search", () -> search(options.buildSearchOptions()));
    }

    private SearchResult search(SearchOptions searchOptions) {

//        SearchResults searchResults = connection.sync().search(index, query.toString(), SearchOptions.builder().build());
//        SearchOptions.builder().returnField(FIELD_NAME).returnField(FIELD_STYLE).build()

        com.redislabs.lettusearch.search.SearchResults searchResults = connection.sync().search(index, searchOptions.toString(), searchOptions);
        logger.debug("found count {}", searchResults.getCount());

        return new LettuceSearchResult(searchResults);
    }

}