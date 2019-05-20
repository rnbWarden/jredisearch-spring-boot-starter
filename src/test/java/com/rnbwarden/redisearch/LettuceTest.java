package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.StubEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.RedisCodec;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.serializer.RedisSerializer;

import static org.junit.Assert.*;

@Ignore // uncomment to test with local redis w/ Search module
public class LettuceTest {

    private LettuceRediSearchClient lettuceRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Class<StubEntity> clazz = StubEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = CompressionCodec.valueCompressor(new SerializedObjectCodec(), CompressionCodec.CompressionType.GZIP);

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

        try {
            lettuceRediSearchClient.dropIndex();
        } catch (Exception e) {
        }

    }
}
