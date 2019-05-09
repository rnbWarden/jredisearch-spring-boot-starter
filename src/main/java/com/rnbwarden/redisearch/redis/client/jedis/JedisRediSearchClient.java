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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public abstract class JedisRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, JRediSearchOptions, SearchableJedisField<E>> {

    private static final Logger logger = LoggerFactory.getLogger(JedisRediSearchClient.class);
    private final Client jRediSearchClient;

    public JedisRediSearchClient(Client jRediSearchClient,
                                 CompressingJacksonSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.jRediSearchClient = jRediSearchClient;
        fieldStrategy.put(RediSearchFieldType.TEXT, SearchableJedisTextField::new);
        fieldStrategy.put(RediSearchFieldType.TAG, SearchableJedisTagField::new);
    }

    @Override
    public RediSearchOptions getRediSearchOptions() {

        return new JRediSearchOptions();
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
                        .map(b -> (byte[])b)
                        .map(redisSerializer::deserialize)
        );
    }

    @Override
    public SearchResults findAll(Integer offset,
                                 Integer limit,
                                 boolean includeContent) {

        offset = ofNullable(offset).orElse(0);
        limit = ofNullable(limit).orElse(defaultMaxValue.intValue());

        JRediSearchOptions options = (JRediSearchOptions) getRediSearchOptions();
        options.setQuery(new Query(ALL_QUERY));
        options.setLimit(Long.valueOf(limit));
        options.setOffset(Long.valueOf(offset));
        options.setNoContent(!includeContent);

        return performTimedOperation("findAll", () -> search(options.buildQuery()));
    }

    @Override
    public SearchResults findByFields(Map<String, String> fieldNameValues,
                                      @Nullable Long offset,
                                      @Nullable Long limit) {

        JRediSearchOptions options = (JRediSearchOptions) getRediSearchOptions();
        options.setLimit(limit);
        options.setOffset(offset);
        return findByFields(fieldNameValues, options);
    }

    @Override
    public SearchResults findByFields(Map<String, String> fieldNameValues,
                                      JRediSearchOptions options) {

        fieldNameValues.forEach((name, value) -> options.addField(name, getField(name).getQuerySyntax(value)));
        return performTimedOperation("searchByFields", () -> search(options.buildQuery()));
    }

    @Override
    public SearchResults find(JRediSearchOptions options) {

        return performTimedOperation("search", () -> search(options.buildQuery()));
    }

    private SearchResults search(Query query) {

        io.redisearch.SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());

        return new JedisSearchResults(searchResult);
    }
}