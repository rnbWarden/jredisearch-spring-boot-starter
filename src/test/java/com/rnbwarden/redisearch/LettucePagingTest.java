package com.rnbwarden.redisearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.CursorOptions;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.StubSkuEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

@Ignore// un-ignore to test with local redis (with Search) module
public class LettucePagingTest {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    private static final String DEFAULT_BRAND = "DND";
    private static final int streamSize = 135726;
    private RediSearchClient rediSearchClient;
    private RedisCodec redisCodec;
    private LettuceRediSearchClient<StubSkuEntity> lettuceRediSearchClient;
    private int keySize = 135722;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        LOGGER.setLevel(Level.INFO);

        Class<StubSkuEntity> clazz = StubSkuEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));
//        rediSearchClient = RediSearchClient.create(RedisURI.create("redis-12764.redisnp.corp.pgcore.com", 12764));
        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 1000L);

        lettuceRediSearchClient.recreateIndex();
        insertTestData();
    }

    @After
    public void tearDown() throws Exception {

        lettuceRediSearchClient.dropIndex();
    }

    private void insertTestData() {

        Map<String, StubSkuEntity> skus = new HashMap<>();
        (new Random()).ints(streamSize).parallel().forEach(random -> {
            String key = "key" + random;
            StubSkuEntity entity = new StubSkuEntity(key, DEFAULT_BRAND);
            lettuceRediSearchClient.save(entity);
            synchronized (this) {
                skus.put(entity.getKey(), entity);
            }
        });
        keySize = skus.keySet().size();
        LOGGER.info("key size = " + keySize);
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {}
    }

    @Test
    public void skuTest() {

        PagingSearchContext context = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(StubSkuEntity.BRAND, DEFAULT_BRAND));
        context.setSortBy("key");

        Collection<String> allResults = new ConcurrentLinkedQueue<>();

        PageableSearchResults<StubSkuEntity> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream()
//        pageableSearchResults.getResultStream(false)
                .forEach(searchResult -> {
                    allResults.add(searchResult.getKey());
                    searchResult.getResult().orElseThrow(AssertionError::new);
                    if (allResults.size() % 500 == 0) {
                        LOGGER.info("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
                    }
                });

        System.out.println("FINISHED with " + allResults.size() + " results"
                + " - total count = " + pageableSearchResults.getTotalResults()
                + " - " + new Date(System.currentTimeMillis()).toString());

//        assertEquals(pageableSearchResults.getTotalResults(), allResults.size(), 0);
        assertEquals(keySize, allResults.size(), 0);
    }

    @Test
    public void multiTest() throws InterruptedException {

        AggregateOptions aggregateOptions = AggregateOptions.builder()
                .operation(com.redislabs.lettusearch.aggregate.Limit.builder().num(Integer.MAX_VALUE).offset(0).build())
                .load("sdoc")
                .build();
        CursorOptions cursorOptions = CursorOptions.builder().count(1000L).build();

        Collection<String> allResults = new ConcurrentLinkedQueue<>();
//        Thread thread = new Thread(() -> {
        StatefulRediSearchConnection<String, Object> conn = rediSearchClient.connect(redisCodec);
        StatefulRediSearchConnection<String, Object> connection = rediSearchClient.connect(redisCodec);
            AggregateWithCursorResults<String, Object> aggregateResults = connection.sync().aggregate("sku", "*", aggregateOptions, cursorOptions);
            aggregateResults.forEach(r -> allResults.add("result found"));
            do {
                System.out.println("found results:" + allResults.size());
//                aggregateResults = connection.sync().cursorRead("sku", aggregateResults.getCursor());    // works
                aggregateResults = conn.sync().cursorRead("sku", aggregateResults.getCursor());      // fails
                aggregateResults.forEach(r -> allResults.add("result found"));
            } while (aggregateResults.size() == 1000);
//        });
//        thread.start();

/**
 //Now that the cursor paging is started, wait briefly and then issue some additional commands
 sleep(1000);
 System.out.println("issuing additional requests...");
 //        IntStream.range(0, 4).mapToObj(i -> String.format("key-%s-test", i)).forEach(key -> {
 //            lettuceRediSearchClient.save(new StubSkuEntity(key, DEFAULT_BRAND));
 //        });
 IntStream.range(0, 4).mapToObj(i -> String.format("key-%s-test", i)).forEach(key -> {
 lettuceRediSearchClient.delete(key);
 });

 while (thread.isAlive()) {
 sleep(1000);
 }
 */
        System.out.println("FINISHED with " + allResults.size() + " results" + " - total count = " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
        assertEquals(keySize, allResults.size(), 0);

        multiTest();
    }

    @Test
    public void multiTestStarter() throws InterruptedException {

        PagingSearchContext context = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(StubSkuEntity.BRAND, DEFAULT_BRAND));
//        context.setSortBy(StubSkuEntity.KEY);

        Set<String> allResults = new LinkedHashSet<>();

        Thread thread = new Thread(() -> {
            PageableSearchResults<StubSkuEntity> pageableSearchResults = lettuceRediSearchClient.findAll(context);//findAll(Integer.MAX_VALUE);//search(context);
            pageableSearchResults.getResultStream(true) //.getResultStream(false)
                    .forEach(searchResult -> {
                        allResults.add(searchResult.getKey());
                        searchResult.getResult().orElseThrow(AssertionError::new);
                        if (allResults.size() % 1000 == 0) {
                            LOGGER.info("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
                        }
                    });
        });
        thread.start();

        sleep(1500);
        System.out.println("issuing additional requests...");
        lettuceRediSearchClient.delete("key-0-test");
//        lettuceRediSearchClient.save(new StubSkuEntity("key-0-test", DEFAULT_BRAND));
//        lettuceRediSearchClient.delete("key-0-test");
//        PageableSearchResults<StubSkuEntity> secondSearchResults = lettuceRediSearchClient.findAll(context);
//        lettuceRediSearchClient.findAll(Integer.MAX_VALUE);
        System.out.println("finished issuing additional requests...");

        while (thread.isAlive()) {
            sleep(1000);
        }
        System.out.println("FINISHED with " + allResults.size() + " results"
                + " - total count = " + allResults.size()
                + " - " + new Date(System.currentTimeMillis()).toString());

        assertEquals(keySize, allResults.size(), 0);
        multiTestStarter();
    }

    @Test
    public void testKeyCount() {

        Long keyCount = lettuceRediSearchClient.getKeyCount();
        assertEquals(keySize, keyCount, 0);
    }

    @Test
    public void testKeyCountPagingSearchContext() {

        PagingSearchContext pagingSearchContext = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(StubSkuEntity.BRAND, DEFAULT_BRAND));
        pagingSearchContext.setPageSize(100000L);
        Long keyCount = lettuceRediSearchClient.getKeyCount(pagingSearchContext);
        assertEquals(keySize, keyCount, 0);
    }
}
