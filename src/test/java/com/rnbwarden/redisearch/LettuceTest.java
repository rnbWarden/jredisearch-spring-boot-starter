package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.SearchContext;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.StubEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
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
public class LettuceTest {

    private LettuceRediSearchClient<StubEntity> lettuceRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Class<StubEntity> clazz = StubEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        RediSearchClient rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));

        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 1000L);

        lettuceRediSearchClient.recreateIndex();
    }

    @After
    public void tearDown() throws Exception {

        lettuceRediSearchClient.dropIndex();
    }

    @Test
    public void test() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        StubEntity stub1 = new StubEntity("key123", "value1", asList("listValue1", "listValue2", "listValue3"));
        StubEntity stub2 = new StubEntity("key456", "value2", asList("listValue4", "listValue2", "listValue5"));
        lettuceRediSearchClient.save(stub1);
        lettuceRediSearchClient.save(stub2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());
        assertNotNull(lettuceRediSearchClient.findByKey(stub1.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(0, 100, false).hasResults());

        SearchResults searchResults = lettuceRediSearchClient.findByFields(newHashMap(COLUMN1, stub1.getColumn1()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<StubEntity> resultEntities = lettuceRediSearchClient.deserialize(searchResults);
        assertNotNull(resultEntities.get(0));

        SearchContext searchContext = new SearchContext();
        searchContext.addField(lettuceRediSearchClient.getField(LIST_COLUMN), SearchOperator.INTERSECTION, "listValue2", "listValue3");
        assertEquals(1, lettuceRediSearchClient.find(searchContext).getResults().size());

        searchContext = new SearchContext();
        searchContext.addField(lettuceRediSearchClient.getField(LIST_COLUMN), SearchOperator.UNION, "listValue2", "listValue3");
        assertEquals(2, lettuceRediSearchClient.find(searchContext).getResults().size());
    }

    @Test
    public void testPaging() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        StubEntity stubEntity1 = new StubEntity("hijklmnop", "value1", emptyList());
        StubEntity stubEntity2 = new StubEntity("abcdefg", "value1", emptyList());
        lettuceRediSearchClient.save(stubEntity1);
        lettuceRediSearchClient.save(stubEntity2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());

        SearchContext searchContext = new SearchContext();
        searchContext.setSortBy(COLUMN1);
        SearchResults searchResults = lettuceRediSearchClient.findByFields(singletonMap(COLUMN1, "value1"), searchContext);

        List<StubEntity> stubEntities = lettuceRediSearchClient.deserialize(searchResults);
        assertEquals(stubEntity2.getColumn1(), stubEntities.get(0).getColumn1());
        assertEquals(stubEntity1.getColumn1(), stubEntities.get(0).getColumn1());
    }
}
