package com.rnbwarden.redisearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.redislabs.lettusearch.aggregate.CursorOptions;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.SortBy;
import com.rnbwarden.redisearch.autoconfiguration.RediSearchLettuceClientAutoConfiguration;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagingSearchContext;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.CurrentPrice;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.redislabs.lettusearch.aggregate.Limit.builder;

@Ignore // un-ignore to test with local redis w/ Search module
public class PagingTest {

    private LettuceRediSearchClient<CurrentPrice> lettuceRediSearchClient;
    private RediSearchClient rediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Class<CurrentPrice> clazz = CurrentPrice.class;
        ObjectMapper objectMapper = new ObjectMapper();
        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();
        rediSearchClient = RediSearchClient.create(RedisURI.create("redis-11925.redisnp.corp.pgcore.com", 11925));
        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 20000L);
    }

    @Test
    public void currentPrice() {

        Set<String> allResults = new HashSet<>();

        PagingSearchContext context = new PagingSearchContext();
        context.addField(lettuceRediSearchClient.getField("brand"), "HAB");
//        context.setOffset(0L);
//        context.setLimit(1000L);
        context.setSortBy("sku");

        PageableSearchResults<CurrentPrice> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream()
                .forEach(searchResult -> {
                    allResults.add(searchResult.getKey());
//                    if (searchResult.getResult().orElse(null) == null) System.out.println(format("CurrentPrice for %s is null!", searchResult.getKey()));
                    if (allResults.size() % 500 == 0) System.out.println("Done with " + allResults.size());
                });

        System.out.println("Finished with " + allResults.size());
    }

    @Test
    public void skuTest() {

        Set<String> allResults = new HashSet<>();

        StatefulRediSearchConnection<String, String> statefulRediSearchConnection = rediSearchClient.connect();
        SortBy sortBy = SortBy.builder().field("sku").direction(SortBy.Direction.Ascending).build();

        final AtomicLong offset = new AtomicLong(0);
        long size = 1000;
        com.redislabs.lettusearch.search.SearchResults<String, String> searchResults;
        do {
            Limit limit = Limit.builder().offset(offset.get()).num(size).build();
            SearchOptions searchOptions = SearchOptions.builder().sortBy(sortBy).limit(limit).build();

            searchResults = statefulRediSearchConnection.sync().search("testskupriceidx2", "@brand:hab", searchOptions);

            System.out.println(String.format("results.size(): %s - totalCount: %s", searchResults.size(), searchResults.getCount()));

            searchResults.stream()
                    .filter(Objects::nonNull)
                    .forEach(r -> {
                        offset.incrementAndGet();
                        allResults.add(r.getDocumentId());
                    });
            System.out.println("allResults.size() = " + allResults.size());
        } while (!searchResults.isEmpty());

        System.out.println("Finished with " + allResults.size());
    }

    @Test
    public void testCursor() {

        String index = "testskupriceidx2";
        StatefulRediSearchConnection<String, String> connection = rediSearchClient.connect();

        SearchOptions searchOptions = SearchOptions.builder().noContent(true).build();
        SearchResults<String, String> searchResults = connection.sync().search(index, "*", searchOptions);
        System.out.println("search results = " + searchResults.getCount());

        AggregateOptions aggregateOptions = AggregateOptions.builder()
                .operation(builder().num(1000).offset(0).build())
//                .operation(Sort.builder().property(SortProperty.builder().property("sku").order(Order.Asc).build()).build())
                .load("sku")
                .build();
        AggregateWithCursorResults<String, String> aggregateResults = connection.sync().aggregate(index, "@brand:hab", aggregateOptions, CursorOptions.builder().build());
        System.out.println("cursor results = " + aggregateResults.getCount());

        AggregateWithCursorResults<String, String> aggregateResults2 = connection.sync().cursorRead(index, aggregateResults.getCursor());
        System.out.println("cursor results = " + aggregateResults2.getCount());
    }
}
