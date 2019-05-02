package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.client.options.JRediSearchOptions;
import com.rnbwarden.redisearch.redis.client.options.RediSearchOptions;
import com.rnbwarden.redisearch.redis.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
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

public abstract class JedisRediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity> extends AbstractRediSearchClient<E, JRediSearchOptions> {

    private static final Logger logger = LoggerFactory.getLogger(JedisRediSearchClient.class);
    private final Client jRediSearchClient;

    public JedisRediSearchClient(Client jRediSearchClient,
                                 CompressingJacksonSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.jRediSearchClient = jRediSearchClient;
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
                .map(SearchableField::getJettisField)
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
                        .map(d -> (byte[]) d.get(SERIALIZED_DOCUMENT))
                        .map(redisSerializer::deserialize));
    }

    @Override
    public SearchResult findAll(Integer offset,
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
    public SearchResult findByFields(Map<String, String> fieldNameValues,
                                     @Nullable Long offset,
                                     @Nullable Long limit) {

        JRediSearchOptions options = (JRediSearchOptions) getRediSearchOptions();
        options.setLimit(limit);
        options.setOffset(offset);
        return findByFields(fieldNameValues, options);
    }

    @Override
    public SearchResult findByFields(Map<String, String> fieldNameValues,
                                     JRediSearchOptions options) {

        fieldNameValues.forEach((name, value) -> options.addField(name, getField(name).getQuerySyntax(value)));
        return performTimedOperation("searchByFields", () -> search(options.buildQuery()));
    }

    @Override
    public SearchResult find(JRediSearchOptions options) {

        return performTimedOperation("search", () -> search(options.buildQuery()));
    }

    private SearchResult search(Query query) {

        io.redisearch.SearchResult searchResult = jRediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());

        return new JedisSearchResult(searchResult);
    }
}