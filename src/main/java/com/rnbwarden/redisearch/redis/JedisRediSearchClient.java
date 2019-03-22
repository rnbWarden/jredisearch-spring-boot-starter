package com.rnbwarden.redisearch.redis;

import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public abstract class JedisRediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity> extends AbstractRediSearchClient<E> {

    private static final Logger logger = LoggerFactory.getLogger(JedisRediSearchClient.class);
    private final Client rediSearchClient;

    public JedisRediSearchClient(Client rediSearchClient,
                                 CompressingJacksonRedisSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.rediSearchClient = rediSearchClient;
    }

    @Override
    protected void checkAndCreateIndex() {

        try {
            rediSearchClient.getInfo();
        } catch (JedisDataException jde) {
            this.rediSearchClient.createIndex(createSchema(), Client.IndexOptions.defaultOptions());
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

        rediSearchClient.dropIndex(true);
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        rediSearchClient.addDocument(entity.getPersistenceKey(), 1, fields, false, true, null);
    }

    @Override
    public void delete(String key) {

        rediSearchClient.deleteDocument(key, true);
    }

    @Override
    public Optional<E> findByKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(rediSearchClient.getDocument(key, false))
                        .map(d -> (byte[]) d.get(SERIALIZED_DOCUMENT))
                        .map(redisSerializer::deserialize));
    }

    protected SearchResult search(Query query) {

        SearchResult searchResult = rediSearchClient.search(query, false);
        logger.debug("found {} totalResults - count {}", searchResult.totalResults, searchResult.docs.stream().filter(Objects::nonNull).count());
        return searchResult;
    }
}