package com.rnbwarden.redisearch;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
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
import java.util.Set;

import static java.lang.String.format;

@Ignore // un-ignore to test with local redis w/ Search module
public class PagingTest {

    private LettuceRediSearchClient<CurrentPrice> lettuceRediSearchClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Class<CurrentPrice> clazz = CurrentPrice.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<?> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec redisCodec = new RediSearchLettuceClientAutoConfiguration.LettuceRedisCodec();

        RediSearchClient rediSearchClient = RediSearchClient.create(RedisURI.create("redis-11925.redisnp.corp.pgcore.com", 11925));

        lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, 20000L);
    }


    @Test
    public void skuTest() {

        Set<String> allResults = new HashSet<>();

        PagingSearchContext context = new PagingSearchContext();
        context.addField(lettuceRediSearchClient.getField("brand"), "HAB");
        context.setOffset(0L);
        context.setLimit(1000L);
        context.setSortBy("sku");

        PageableSearchResults<CurrentPrice> pageableSearchResults = lettuceRediSearchClient.search(context);
        pageableSearchResults.getResultStream()
                .forEach(searchResult -> {
                    allResults.add(searchResult.getKey());

                    CurrentPrice currentPrice = searchResult.getResult().orElse(null);
                    if (currentPrice == null) {
                        System.out.println(format("CurrentPrice for %s is null!", searchResult.getKey()));
                    }
                    if (allResults.size() % 500 == 0) System.out.println("Done with " + allResults.size());
                });
    }
}
