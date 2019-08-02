package com.rnbwarden.redisearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
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

import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static java.lang.String.format;

//import org.slf4j.Logger;

@Ignore // un-ignore to test with local redis w/ Search module
public class LettuceSkuPagingTest {

//    private static final Logger LOGGER = LoggerFactory.getLogger(LettuceSkuTest.class);

    private LettuceRediSearchClient<StubSkuEntity> lettuceRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Class<StubSkuEntity> clazz = StubSkuEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        RediSearchClient rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));
        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 1000L);

        lettuceRediSearchClient.recreateIndex();
        insertTestData();
    }

    @After
    public void tearDown() throws Exception {

        lettuceRediSearchClient.dropIndex();
    }

    private void insertTestData() {

        int streamSize = 135726;
        (new Random()).ints(streamSize).forEach(random -> {
            String key = "key" + random;
            lettuceRediSearchClient.save(new StubSkuEntity(key, "DND"));
        });
    }

    /**
     * @Test public void skuTest() {
     * <p>
     * Set<String> inserts = new HashSet<>();
     * int streamSize = 135726;
     * (new Random()).ints(streamSize).forEach(random -> {
     * String key = "key" + random;
     * lettuceRediSearchClient.save(new StubSkuEntity(key, "DND"));
     * inserts.add(key);
     * });
     * <p>
     * System.out.println("inserts.size() = " + inserts.size());
     * int insertsSize = inserts.size();
     * inserts.clear();
     * <p>
     * Set<String> allResults = new HashSet<>();
     * <p>
     * long offset = 0;
     * long size = 500;
     * while (offset < insertsSize) {
     * <p>
     * //            SortBy sortBy = SortBy.builder().field(StubSkuEntity.KEY).direction(SortBy.Direction.Ascending).build();
     * //            SearchOptions.builder().sortBy(sortBy)
     * //                    .limit(Limit.builder().offset(offset).num(size).build())
     * //                    .build();
     * //            Map<String, String> fieldNameValues = Collections.singletonMap(StubSkuEntity.BRAND, "DND");
     * SearchContext rediSearchOptions = new SearchContext();
     * rediSearchOptions.addField(lettuceRediSearchClient.getField(StubSkuEntity.BRAND), "DND");
     * <p>
     * SearchResults sr = lettuceRediSearchClient.find(rediSearchOptions);
     * List<SearchResult<String, Object>> results = sr.getResults();
     * System.out.println("results.size() = " + results.size());
     * <p>
     * results.forEach(r -> allResults.add(r.getId()));
     * System.out.println("allResults.size() = " + allResults.size());
     * <p>
     * offset = offset + results.size();
     * }
     * }
     */

    @Test
    public void skuTest() {

        Set<String> allResults = new HashSet<>();

        PagingSearchContext context = new PagingSearchContext();

        context.addField(lettuceRediSearchClient.getField("brand"), "DND");
//        context.setOffset(0L);
//        context.setLimit(10000000000L);
        context.setSortBy("key");

        PageableSearchResults<StubSkuEntity> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream()
//        pageableSearchResults.getResultStream(false)
                .forEach(searchResult -> {
                    allResults.add(searchResult.getKey());

                    StubSkuEntity stubSkuEntity = searchResult.getResult().orElse(null);
                    if (stubSkuEntity == null) {
                        //LOGGER.info(format("StubSkuEntity for %s is null!", searchResult.getKey()));
                        System.out.println(format("StubSkuEntity for %s is null!", searchResult.getKey()));
                    }
//                    if (allResults.size() % 500 == 0) LOGGER.info("Done with " + allResults.size());
                    if (allResults.size() % 500 == 0) {
                        System.out.println("Done with " + allResults.size() + " - " + new Date(System.currentTimeMillis()).toString());
                    }
                });

        System.out.println("FINISHED with " + allResults.size()
                + " - total count = " + pageableSearchResults.getTotalResults()
                + " - " + new Date(System.currentTimeMillis()).toString());
    }
}
