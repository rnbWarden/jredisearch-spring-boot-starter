package com.rnbwarden.redisearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.CursorOptions;
import com.redislabs.lettusearch.aggregate.SortProperty;
import com.redislabs.lettusearch.search.SearchOptions;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagingSearchContext;
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
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;

import static com.rnbwarden.redisearch.entity.StubEntity.COLUMN1;
import static com.rnbwarden.redisearch.entity.StubEntity.LIST_COLUMN;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.util.Maps.newHashMap;
import static org.junit.Assert.*;

@Ignore // un-ignore to test with local redis w/ Search module
public class LettuceTest {

    private LettuceRediSearchClient<StubEntity> lettuceRediSearchClient;
    private RediSearchClient rediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Class<StubEntity> clazz = StubEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));
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
        assertTrue(lettuceRediSearchClient.findAll(100).hasResults());

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

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Set<String> allKeys = new HashSet<>();
        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        (new Random()).ints(35726).forEach(random -> {
            String key = "key" + random;
            allKeys.add(key);
            lettuceRediSearchClient.save(new StubEntity(key, key + "-value1", emptyList()));
        });

        Set<String> allResults = new HashSet<>();

        PagingSearchContext context = new PagingSearchContext();
        context.addField(lettuceRediSearchClient.getField(COLUMN1), "value1");
//        context.setOffset(0L);
//        context.setLimit(10000000000L);
        context.setSortBy(COLUMN1);

        PageableSearchResults<StubEntity> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream()
//        pageableSearchResults.getResultStream(false)
                .forEach(searchResult -> {
                    synchronized (this) {
                        allResults.add(searchResult.getKey());
                    }
                    if (allResults.size() % 500 == 0) {
                        System.out.println("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
                    }
                });

        System.out.println("FINISHED with " + allResults.size()
                + " - total count = " + pageableSearchResults.getTotalResults()
                + " - total keys = " + allKeys.size()
                + " - " + new Date(System.currentTimeMillis()).toString());
    }

    @Test
    public void testCursorPaging() {

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Set<String> allKeys = new HashSet<>();
        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        (new Random()).ints(5726).forEach(random -> {
            String key = "key" + random;
            allKeys.add(key);
            lettuceRediSearchClient.save(new StubEntity(key, key + "-value1", emptyList()));
        });

        Set<String> allResults = new HashSet<>();

        PagingSearchContext context = new PagingSearchContext();
        context.addField(lettuceRediSearchClient.getField(COLUMN1), "value1");
        context.setSortBy(COLUMN1);
        context.setUseClientSidePaging(false);

        PageableSearchResults<StubEntity> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream(true)
//        pageableSearchResults.getResultStream()
                .forEach(searchResult -> {
                    synchronized (this) {
                        allResults.add(searchResult.getKey());
                    }
                    if (allResults.size() % 500 == 0) {
                        System.out.println("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
                    }
                });

        root.info("FINISHED with " + allResults.size()
                + " - total count = " + pageableSearchResults.getTotalResults()
                + " - total keys = " + allKeys.size()
                + " - " + new Date(System.currentTimeMillis()).toString());
    }

    @Test
    public void testFindAll() {

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Set<String> allKeys = new HashSet<>();
        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        (new Random()).ints(5726).forEach(random -> {
            String key = "key" + random;
            allKeys.add(key);
            lettuceRediSearchClient.save(new StubEntity(key, key + "-value1", emptyList()));
        });

        Set<String> allResults = new HashSet<>();

        while (allResults.size() < allKeys.size()) {
            lettuceRediSearchClient.findAll(allResults.size(), 500, true).getResultStream()
                    .forEach(searchResult -> addResult(allResults, searchResult));
        }

        root.info("FINISHED with " + allResults.size()
                + " - total keys = " + allKeys.size()
                + " - " + new Date(System.currentTimeMillis()).toString());
    }

    private void addResult(Set<String> allResults, PagedSearchResult<StubEntity> searchResult) {

        allResults.add(searchResult.getKey());
        if (allResults.size() % 500 == 0) {
            System.out.println("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
        }
    }
}
