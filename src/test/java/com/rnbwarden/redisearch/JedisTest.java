package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.client.jedis.JedisRediSearchClient;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.StubEntity;
import io.redisearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.rnbwarden.redisearch.entity.StubEntity.COLUMN1;
import static com.rnbwarden.redisearch.entity.StubEntity.LIST_COLUMN;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
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
        assertTrue(jedisRediSearchClient.findAll(100).hasResults());

        SearchResults searchResults = jedisRediSearchClient.findByFields(newHashMap(COLUMN1, stub1.getColumn1()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<StubEntity> resultEntities = jedisRediSearchClient.deserialize(searchResults);
        assertNotNull(resultEntities.get(0));

        SearchContext searchContext = new SearchContext();
        searchContext.addField(jedisRediSearchClient.getField(LIST_COLUMN), SearchOperator.INTERSECTION, "listValue2", "listValue3");
        assertEquals(1, jedisRediSearchClient.find(searchContext).getResults().size());

        searchContext = new SearchContext();
        searchContext.addField(jedisRediSearchClient.getField(LIST_COLUMN), SearchOperator.UNION, "listValue2", "listValue3");
        assertEquals(2, jedisRediSearchClient.find(searchContext).getResults().size());
    }

    @Test
    public void testPaging() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        StubEntity stubEntity1 = new StubEntity("hijklmnop", "value1", emptyList());
        StubEntity stubEntity2 = new StubEntity("abcdefg", "value1", emptyList());
        jedisRediSearchClient.save(stubEntity1);
        jedisRediSearchClient.save(stubEntity2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        PagingSearchContext pagingSearchContext = jedisRediSearchClient.getPagingSearchContextWithFields(singletonMap(COLUMN1, "value1"));
        PageableSearchResults<StubEntity> searchResults = jedisRediSearchClient.search(pagingSearchContext);

        List<StubEntity> stubEntities = searchResults.getResultStream(false)
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toList());
        assertEquals(stubEntity2.getColumn1(), stubEntities.get(0).getColumn1());
        assertEquals(stubEntity1.getColumn1(), stubEntities.get(0).getColumn1());
    }

    @Test
    public void testSorting() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());
        IntStream.range(1, 10000).forEach(i -> jedisRediSearchClient.save(new StubEntity("zyxwvut" + i, i + "value", emptyList())));

        assertEquals(9999, (long) jedisRediSearchClient.getKeyCount());

        PageableSearchResults<StubEntity> searchResults = jedisRediSearchClient.findAll(100000);
        assertEquals(9999, searchResults.getResultStream().count());

        List<StubEntity> stubEntities = searchResults.getResultStream(false)
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toList());
        assertEquals("zyxwvut9999", stubEntities.get(0).getKey());
        assertEquals("zyxwvut9998", stubEntities.get(1).getKey());
    }

    @Test
    public void testMultiGet() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        List<String> keys = new ArrayList<>();
        IntStream.range(1, 100).forEach(i -> {
            StubEntity entity = new StubEntity("zyxwvut" + i, i + "value", emptyList());
            keys.add(entity.getPersistenceKey());
            jedisRediSearchClient.save(entity);
        });

        List<String> fetchKeys = new ArrayList<>();
        fetchKeys.add(keys.get(7));
        fetchKeys.add(keys.get(36));
        fetchKeys.add(keys.get(44));
        fetchKeys.add(keys.get(59));
        fetchKeys.add(keys.get(73));
        fetchKeys.add(keys.get(81));
        fetchKeys.add("unknown-key");

        List<StubEntity> results = jedisRediSearchClient.findByKeys(fetchKeys);
        assertEquals(fetchKeys.size() - 1, results.size());

        Map<String, StubEntity> resultsMap = results.stream().collect(Collectors.toMap(StubEntity::getPersistenceKey, identity()));

        fetchKeys.stream()
                .filter(key -> !key.equals(fetchKeys.get(fetchKeys.size() - 1)))
                .forEach(key -> assertNotNull(resultsMap.get(key)));

    }
}
