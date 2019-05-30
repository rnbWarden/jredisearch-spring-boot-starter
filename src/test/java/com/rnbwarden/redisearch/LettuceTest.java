package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.StubEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.List;

import static org.assertj.core.util.Maps.newHashMap;
import static org.junit.Assert.*;

@Ignore // un-ignore to test with local redis w/ Search module
public class LettuceTest {

    private LettuceRediSearchClient lettuceRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Class<StubEntity> clazz = StubEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        RediSearchClient rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));

        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 1000L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test() {

        lettuceRediSearchClient.recreateIndex();

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        StubEntity stub = new StubEntity("key123", "value1");
        lettuceRediSearchClient.save(stub);

        assertEquals(1, (long) lettuceRediSearchClient.getKeyCount());
        assertNotNull(lettuceRediSearchClient.findByKey(stub.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(0, 100, false).hasResults());

        SearchResults searchResults = lettuceRediSearchClient.findByFields(newHashMap("column1", "value1"));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<StubEntity> resultEntities = lettuceRediSearchClient.deserialize(searchResults);
        assertNotNull(resultEntities.get(0));

        try {
            lettuceRediSearchClient.dropIndex();
        } catch (Exception e) {
        }

    }
}
