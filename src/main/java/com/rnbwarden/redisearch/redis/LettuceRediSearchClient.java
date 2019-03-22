package com.rnbwarden.redisearch.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.*;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static io.redisearch.querybuilder.QueryBuilder.intersect;
import static java.lang.String.format;
import static java.util.Arrays.stream;

public abstract class LettuceRediSearchClient<E extends /**RediSearchEntity &*/RedisSearchableEntity> extends AbstractRediSearchClient<E> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final StatefulRediSearchConnection<String, Object> connection;
    private final String index;

    public LettuceRediSearchClient(StatefulRediSearchConnection<String, Object> connection,
                                   CompressingJacksonRedisSerializer<E> redisSerializer) {

        super(redisSerializer);
        this.connection = connection;
        this.index = getIndex(clazz);
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

        //WARDEN: figure this out once we get an answer from Guy
        return Optional.empty();
        /**
        return performTimedOperation("findByKey",
                () -> ofNullable(connection.sync().get(key))
                        .map(d -> (byte[]) d.get(SERIALIZED_DOCUMENT))
                        .map(redisSerializer::deserialize));
         */
    }

    protected SearchResult search(Query query) {

        SearchResults searchResults = connection.sync().search(index, query.toString(), SearchOptions.builder().build());
        logger.debug("found count {}", searchResults.getCount());

        //WARDEN: figure this out once we get an answer from Guy
        return null;//searchResults;
    }

}