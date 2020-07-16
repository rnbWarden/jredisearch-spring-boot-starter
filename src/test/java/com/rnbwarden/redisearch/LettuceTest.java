package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.config.factorybean.RediSearchLettuceClientFactoryBean;
import com.rnbwarden.redisearch.entity.Brand;
import com.rnbwarden.redisearch.entity.ProductEntity;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.SkuEntity;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.rnbwarden.redisearch.entity.ProductEntity.*;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LettuceTest {

    private LettuceRediSearchClient<ProductEntity> lettuceRediSearchClient;

    @Before
    public void setUp() {

        createRediSearchClient();
    }

    @After
    public void tearDown() throws Exception {

        lettuceRediSearchClient.dropIndex();
    }

    private void createRediSearchClient() {

        Class<ProductEntity> clazz = ProductEntity.class;
        ObjectMapper objectMapper = new ObjectMapper();

        RedisSerializer<ProductEntity> redisSerializer = new CompressingJacksonSerializer<>(clazz, objectMapper);
        RedisCodec<String, Object> redisCodec = new RediSearchLettuceClientFactoryBean.LettuceRedisCodec();

        RediSearchClient rediSearchClient = RediSearchClient.create(RedisURI.create("localhost", 6379));
        lettuceRediSearchClient = new LettuceRediSearchClient<>(clazz, rediSearchClient, redisCodec, redisSerializer, 1000L);
    }

    @Test
    public void test() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                Collections.emptyList()
        );

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))),
                Collections.emptyList()
        );

        lettuceRediSearchClient.save(product1);
        lettuceRediSearchClient.save(product2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());

        assertNotNull(lettuceRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = lettuceRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<ProductEntity> resultEntities = lettuceRediSearchClient.deserialize(searchResults);
        assertEquals(product1, resultEntities.get(0));

        SearchContext<ProductEntity> searchContext = new SearchContext<>();
        searchContext.addField(lettuceRediSearchClient.getField(SKUS), SearchOperator.INTERSECTION, "f01", "f02");
        assertEquals(1, lettuceRediSearchClient.find(searchContext).getResults().size());

        searchContext = new SearchContext<>();
        searchContext.addField(lettuceRediSearchClient.getField(SKUS), SearchOperator.UNION, "f01", "b02");
        assertEquals(2, lettuceRediSearchClient.find(searchContext).getResults().size());
    }

    @Test
    public void testPaging() {

        int max = 10000;
        String namePrefix = "FALCON-";
        Brand brand = Brand.ADIDAS;

        assertEquals(0, lettuceRediSearchClient.getKeyCount(), 0);

        lettuceRediSearchClient.save(new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                Collections.emptyList()));

        saveProductsInRange(max, namePrefix, brand);

        assertEquals(max + 1, lettuceRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(BRAND, Brand.ADIDAS.toString()));
        PageableSearchResults<ProductEntity> searchResults = lettuceRediSearchClient.search(pagingSearchContext);

        List<ProductEntity> products = searchResults.resultStream()
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toList());

        assertEquals(max, products.size());
        products.forEach(product -> {
            assertTrue(product.getId().startsWith("id"));
            assertTrue(product.getArticleNumber().startsWith(namePrefix));
            assertEquals(brand, product.getBrand());
        });
    }

    @Test
    public void testClientSidePaging() {

        int max = 10000;
        String namePrefix = "FALCON-";
        Brand brand = Brand.ADIDAS;

        assertEquals(0, lettuceRediSearchClient.getKeyCount(), 0);

        lettuceRediSearchClient.save(new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                Collections.emptyList()));

        saveProductsInRange(max, namePrefix, brand);

        assertEquals(max + 1, lettuceRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(BRAND, Brand.ADIDAS.toString()));
        pagingSearchContext.setUseClientSidePaging(true);
        PageableSearchResults<ProductEntity> searchResults = lettuceRediSearchClient.search(pagingSearchContext);

        List<ProductEntity> products = searchResults.resultStream()
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toList());

        assertEquals(max, products.size());
        products.forEach(product -> {
            assertTrue(product.getId().startsWith("id"));
            assertTrue(product.getArticleNumber().startsWith(namePrefix));
            assertEquals(brand, product.getBrand());
        });
    }

    private void saveProductsInRange(int max, String namePrefix, Brand brand) {

        IntStream.range(0, max)/*.parallel()*/.forEach(i -> {
            ProductEntity product = new ProductEntity("id" + i, namePrefix + i, brand, Collections.emptyList(), Collections.emptyList());
            lettuceRediSearchClient.save(product);
        });
    }

    @Test
    public void testFindAll() {

        int max = 3726;
        String namePrefix = "FALCON-";
        Brand brand = Brand.NIKE;

        assertEquals(0, lettuceRediSearchClient.getKeyCount(), 0);
        saveProductsInRange(max, namePrefix, brand);
        assertEquals(max, lettuceRediSearchClient.getKeyCount(), 0);

        Set<ProductEntity> products = lettuceRediSearchClient.findAll(1000000).resultStream()
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toSet());

        assertEquals(max, products.size());
    }

    @Test
    public void testSorting() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());
        int max = 1000;

        lettuceRediSearchClient.save(new ProductEntity("id-ZZZ01", "ZZZ01", Brand.NIKE, Collections.emptyList(), Collections.emptyList()));
        saveProductsInRange(max - 2, "TEST-", Brand.NIKE);
        lettuceRediSearchClient.save(new ProductEntity("id-AAA01", "AAA01", Brand.NIKE, Collections.emptyList(), Collections.emptyList()));

        assertEquals(max, lettuceRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = new PagingSearchContext<>();
        pagingSearchContext.setSortBy(ARTICLE_NUMBER);
        pagingSearchContext.setSortAscending(true);
        PageableSearchResults<ProductEntity> searchResults = lettuceRediSearchClient.findAll(pagingSearchContext);

        List<ProductEntity> products = searchResults.resultStream()
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toList());

        assertEquals(max, products.size());
        assertTrue(products.get(0).getArticleNumber().startsWith("AAA"));
        assertTrue(products.get(products.size() - 1).getArticleNumber().startsWith("ZZZ"));
    }

    @Test
    public void testMultiGet() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        List<String> keys = new ArrayList<>();
        IntStream.range(1, 100).forEach(i -> {
            ProductEntity entity = new ProductEntity("zyxwvut" + i, i + "value", Brand.NIKE, emptyList(), Collections.emptyList());
            keys.add(entity.getPersistenceKey());
            lettuceRediSearchClient.save(entity);
        });

        List<String> fetchKeys = new ArrayList<>();
        fetchKeys.add(keys.get(7));
        fetchKeys.add(keys.get(36));
        fetchKeys.add(keys.get(44));
        fetchKeys.add(keys.get(59));
        fetchKeys.add(keys.get(73));
        fetchKeys.add(keys.get(81));
        fetchKeys.add("unknown-key");

        List<ProductEntity> results = lettuceRediSearchClient.findByKeys(fetchKeys);
        assertEquals(fetchKeys.size() - 1, results.size());

        Map<String, ProductEntity> resultsMap = results.stream().collect(Collectors.toMap(ProductEntity::getPersistenceKey, identity()));

        fetchKeys.stream()
                .filter(key -> !key.equals(fetchKeys.get(fetchKeys.size() - 1)))
                .forEach(key -> assertNotNull(resultsMap.get(key)));

    }

    @Test
    public void testKeyCount() {

        int keySize = 9845;
        saveProductsInRange(keySize, "TEST-", Brand.NIKE);
        Long keyCount = lettuceRediSearchClient.getKeyCount();
        assertEquals(keySize, keyCount, 0);
    }

    @Test
    public void testKeyCountPagingSearchContext() {

        int keySize = 23464;
        saveProductsInRange(keySize, "TEST-", Brand.NIKE);

        PagingSearchContext<ProductEntity> pagingSearchContext = lettuceRediSearchClient.getPagingSearchContextWithFields(Map.of(ProductEntity.BRAND, Brand.NIKE.toString()));
        pagingSearchContext.setPageSize(100000L);
        Long keyCount = lettuceRediSearchClient.getKeyCount(pagingSearchContext);
        assertEquals(keySize, keyCount, 0);
    }

    @Test
    public void testEscapingValues() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON01$", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))),
                Collections.emptyList());

        lettuceRediSearchClient.save(product1);
        lettuceRediSearchClient.save(product2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());

        assertNotNull(lettuceRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = lettuceRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, lettuceRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    //@Test(expected=IllegalArgumentException.class)
    public void testEscapingValuesWithBackslash() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON\\01$", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))
                ), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))), Collections.emptyList());

        lettuceRediSearchClient.save(product1);
        lettuceRediSearchClient.save(product2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());

        assertNotNull(lettuceRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = lettuceRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, lettuceRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    public void testEscapingValuesWithSpaces() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON 01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))
                ), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))), Collections.emptyList());

        lettuceRediSearchClient.save(product1);
        lettuceRediSearchClient.save(product2);

        assertEquals(2, (long) lettuceRediSearchClient.getKeyCount());

        assertNotNull(lettuceRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(lettuceRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = lettuceRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, lettuceRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    public void testSearchByFields() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))), Collections.emptyList());

        lettuceRediSearchClient.save(product1);
        lettuceRediSearchClient.save(product2);

        PageableSearchResults<ProductEntity> results = lettuceRediSearchClient.searchByFields(Map.of(BRAND, Brand.NIKE.toString()));

        List<String> keys = results.resultStream()
                .map(PagedSearchResult::getKey)
                .collect(Collectors.toList());

        //System.out.println("test"); //breakpoint here to validate connection(s) closed via CLIENT LIST on redis server-side
    }

    @Test
    public void testNonSearchableFields() {

        assertEquals(0, (long) lettuceRediSearchClient.getKeyCount());

        String attribute1 = "includes shoelaces!";
        ProductEntity product1 = new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                List.of(attribute1));
        lettuceRediSearchClient.save(product1);

        assertEquals(1, (long) lettuceRediSearchClient.getKeyCount());

        SearchContext<ProductEntity> searchContext = lettuceRediSearchClient.getSearchContextWithFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        searchContext.addResultField(ProductEntity.ATTRIBUTES);

        SearchResults<ProductEntity> searchResults = lettuceRediSearchClient.find(searchContext);
        assertEquals(1, searchResults.getResults().size());
        assertTrue(searchResults.hasResults());

        searchResults.forEach(result -> assertEquals(attribute1, result.getFieldValue(ATTRIBUTES)));
    }

}
