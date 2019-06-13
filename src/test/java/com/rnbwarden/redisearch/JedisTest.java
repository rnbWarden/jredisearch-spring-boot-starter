package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.RediSearchOptions;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.jedis.JedisRediSearchClient;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.StubEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.redisearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.List;

import static com.rnbwarden.redisearch.entity.StubEntity.COLUMN1;
import static com.rnbwarden.redisearch.entity.StubEntity.LIST_COLUMN;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.util.Maps.newHashMap;
import static org.junit.Assert.*;

@Ignore // un-ignore to test with local redis w/ Search module
public class JedisTest {

    private JedisRediSearchClient<StubEntity> jedisRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Class<StubEntity> clazz = StubEntity.class;
        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, new ObjectMapper());
        Client rediSearchClient = new Client("stub", "localhost", 6379);
        jedisRediSearchClient = new JedisRediSearchClient(clazz, rediSearchClient, redisSerializer, 1000L);

        jedisRediSearchClient.recreateIndex();
    }

    @After
    public void tearDown() throws Exception {

        jedisRediSearchClient.dropIndex();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        StubEntity stub1 = new StubEntity("key123", "value1", asList("listValue1", "listValue2", "listValue3"));
        StubEntity stub2 = new StubEntity("key456", "value2", asList("listValue4", "listValue2", "listValue5"));
        jedisRediSearchClient.save(stub1);
        jedisRediSearchClient.save(stub2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());
        assertNotNull(jedisRediSearchClient.findByKey(stub1.getPersistenceKey()));
        assertTrue(jedisRediSearchClient.findAll(0, 100, false).hasResults());

        SearchResults searchResults = jedisRediSearchClient.findByFields(newHashMap(COLUMN1, stub1.getColumn1()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<StubEntity> resultEntities = jedisRediSearchClient.deserialize(searchResults);
        assertNotNull(resultEntities.get(0));

        RediSearchOptions options = new RediSearchOptions();
        options.addField(jedisRediSearchClient.getField(LIST_COLUMN), SearchOperator.INTERSECTION, "listValue2", "listValue3");
        assertEquals(1, jedisRediSearchClient.find(options).getResults().size());

        options = new RediSearchOptions();
        options.addField(jedisRediSearchClient.getField(LIST_COLUMN), SearchOperator.UNION, "listValue2", "listValue3");
        assertEquals(2, jedisRediSearchClient.find(options).getResults().size());
    }

    @Test
    public void testPaging() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        StubEntity stubEntity1 = new StubEntity("hijklmnop", "value1", emptyList());
        StubEntity stubEntity2 = new StubEntity("abcdefg", "value1", emptyList());
        jedisRediSearchClient.save(stubEntity1);
        jedisRediSearchClient.save(stubEntity2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        RediSearchOptions options = new RediSearchOptions();
        options.setSortBy(COLUMN1);
        SearchResults searchResults = jedisRediSearchClient.findByFields(singletonMap(COLUMN1, "value1"), options);

        List<StubEntity> stubEntities = jedisRediSearchClient.deserialize(searchResults);
        assertEquals(stubEntity2.getColumn1(), stubEntities.get(0).getColumn1());
        assertEquals(stubEntity1.getColumn1(), stubEntities.get(0).getColumn1());
    }
}
