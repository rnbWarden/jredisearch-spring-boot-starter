package com.rnbwarden.redisearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.client.jedis.JedisRediSearchClient;
import com.rnbwarden.redisearch.entity.Brand;
import com.rnbwarden.redisearch.entity.ProductEntity;
import com.rnbwarden.redisearch.entity.SearchOperator;
import com.rnbwarden.redisearch.entity.SkuEntity;
import io.redisearch.client.Client;
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

public class JedisTest {

    private JedisRediSearchClient<ProductEntity> jedisRediSearchClient;
    private Client rediSearchClient;

    @Before
    public void setUp() {

        createJedisRediSearchClient();
    }

    @After
    public void tearDown() throws Exception {

        try {
            jedisRediSearchClient.dropIndex();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createJedisRediSearchClient() {

        Class<ProductEntity> clazz = ProductEntity.class;
        RedisSerializer<ProductEntity> redisSerializer = new CompressingJacksonSerializer<>(clazz, new ObjectMapper());
        rediSearchClient = new Client("product", "localhost", 6379);
        jedisRediSearchClient = new JedisRediSearchClient<>(clazz, rediSearchClient, redisSerializer, 1000L);
    }

    @Test
    public void test() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))), Collections.emptyList());

        jedisRediSearchClient.save(product1);
        jedisRediSearchClient.save(product2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        assertNotNull(jedisRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(jedisRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = jedisRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        List<ProductEntity> resultEntities = jedisRediSearchClient.deserialize(searchResults);
        assertEquals(product1, resultEntities.get(0));

        SearchContext<ProductEntity> searchContext = new SearchContext<>();
        searchContext.addField(jedisRediSearchClient.getField(SKUS), SearchOperator.INTERSECTION, "f01", "f02");
        assertEquals(1, jedisRediSearchClient.find(searchContext).getResults().size());

        searchContext = new SearchContext<>();
        searchContext.addField(jedisRediSearchClient.getField(SKUS), SearchOperator.UNION, "f01", "b02");
        assertEquals(2, jedisRediSearchClient.find(searchContext).getResults().size());
    }

    @Test
    public void testPaging() {

        int max = 10000;
        String namePrefix = "FALCON-";
        Brand brand = Brand.ADIDAS;

        assertEquals(0, jedisRediSearchClient.getKeyCount(), 0);

        jedisRediSearchClient.save(new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))), Collections.emptyList()));

        saveProductsInRange(max, namePrefix, brand);

        assertEquals(max + 1, jedisRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = jedisRediSearchClient.getPagingSearchContextWithFields(Map.of(BRAND, Brand.ADIDAS.toString()));
        PageableSearchResults<ProductEntity> searchResults = jedisRediSearchClient.search(pagingSearchContext);

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

        assertEquals(0, jedisRediSearchClient.getKeyCount(), 0);

        jedisRediSearchClient.save(new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))), Collections.emptyList()));

        saveProductsInRange(max, namePrefix, brand);

        assertEquals(max + 1, jedisRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = jedisRediSearchClient.getPagingSearchContextWithFields(Map.of(BRAND, Brand.ADIDAS.toString()));
        pagingSearchContext.setUseClientSidePaging(true);
        PageableSearchResults<ProductEntity> searchResults = jedisRediSearchClient.search(pagingSearchContext);

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
            jedisRediSearchClient.save(product);
        });
    }

    @Test
    public void testFindAll() {

        int max = 3726;
        String namePrefix = "FALCON-";
        Brand brand = Brand.NIKE;

        assertEquals(0, jedisRediSearchClient.getKeyCount(), 0);
        saveProductsInRange(max, namePrefix, brand);
        assertEquals(max, jedisRediSearchClient.getKeyCount(), 0);

        Set<ProductEntity> products = jedisRediSearchClient.findAll(1000000).resultStream()
                .map(PagedSearchResult::getResult)
                .map(Optional::get)
                .collect(Collectors.toSet());

        assertEquals(max, products.size());
    }

    @Test
    public void testSorting() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());
        int max = 1000;

        jedisRediSearchClient.save(new ProductEntity("id-ZZZ01", "ZZZ01", Brand.NIKE, Collections.emptyList(), Collections.emptyList()));
        saveProductsInRange(max - 2, "TEST-", Brand.NIKE);
        jedisRediSearchClient.save(new ProductEntity("id-AAA01", "AAA01", Brand.NIKE, Collections.emptyList(), Collections.emptyList()));

        assertEquals(max, jedisRediSearchClient.getKeyCount(), 0);

        PagingSearchContext<ProductEntity> pagingSearchContext = new PagingSearchContext<>();
        pagingSearchContext.setSortBy(ARTICLE_NUMBER);
        pagingSearchContext.setSortAscending(true);
        PageableSearchResults<ProductEntity> searchResults = jedisRediSearchClient.findAll(pagingSearchContext);

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

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        List<String> keys = new ArrayList<>();
        IntStream.range(1, 100).forEach(i -> {
            ProductEntity entity = new ProductEntity("zyxwvut" + i, i + "value", Brand.NIKE, emptyList(), Collections.emptyList());
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

        List<ProductEntity> results = jedisRediSearchClient.findByKeys(fetchKeys);
        assertEquals(fetchKeys.size() - 1, results.size());

        Map<String, ProductEntity> resultsMap = results.stream().collect(Collectors.toMap(ProductEntity::getPersistenceKey, identity()));

        fetchKeys.stream()
                .filter(key -> !key.equals(fetchKeys.get(fetchKeys.size() - 1)))
                .forEach(key -> assertNotNull(resultsMap.get(key)));

    }

    @Test
    public void testKeyCountPagingSearchContext() {

        int keySize = 23464;
        saveProductsInRange(keySize, "TEST-", Brand.NIKE);

        PagingSearchContext<ProductEntity> pagingSearchContext = jedisRediSearchClient.getPagingSearchContextWithFields(Map.of(ProductEntity.BRAND, Brand.NIKE.toString()));
        pagingSearchContext.setPageSize(100000L);
        Long keyCount = jedisRediSearchClient.getKeyCount(pagingSearchContext);
        assertEquals(keySize, keyCount, 0);
    }

    @Test
    public void testEscapingValues() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON01$", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))
                ), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))
                ), Collections.emptyList());

        jedisRediSearchClient.save(product1);
        jedisRediSearchClient.save(product2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        assertNotNull(jedisRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(jedisRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = jedisRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, jedisRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    //@Test(expected=IllegalArgumentException.class)
    public void testEscapingValuesWithBackslash() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON\\01$", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))
                ), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))
                ), Collections.emptyList());

        jedisRediSearchClient.save(product1);
        jedisRediSearchClient.save(product2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        assertNotNull(jedisRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(jedisRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = jedisRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, jedisRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    public void testEscapingValuesWithSpaces() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        ProductEntity product1 = new ProductEntity("id123", "$FALCON 01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))
                ), Collections.emptyList());

        ProductEntity product2 = new ProductEntity("id234", "BLAZE-X", Brand.NIKE,
                List.of(new SkuEntity("b01", Map.of("color", "red", "price", "79.99")),
                        new SkuEntity("b02", Map.of("color", "orange", "price", "79.99"))
                ), Collections.emptyList());

        jedisRediSearchClient.save(product1);
        jedisRediSearchClient.save(product2);

        assertEquals(2, (long) jedisRediSearchClient.getKeyCount());

        assertNotNull(jedisRediSearchClient.findByKey(product1.getPersistenceKey()));
        assertTrue(jedisRediSearchClient.findAll(100).hasResults());

        SearchResults<ProductEntity> searchResults = jedisRediSearchClient.findByFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));
        assertEquals(product1, jedisRediSearchClient.deserialize(searchResults).get(0));
    }

    @Test
    public void testNonSearchableFields() {

        assertEquals(0, (long) jedisRediSearchClient.getKeyCount());

        String attribute1 = "includes shoelaces!";
        ProductEntity product1 = new ProductEntity("id123", "FALCON01", Brand.NIKE,
                List.of(new SkuEntity("f01", Map.of("color", "black", "price", "99.99")),
                        new SkuEntity("f02", Map.of("color", "white", "price", "99.99"))),
                List.of(attribute1));
        jedisRediSearchClient.save(product1);

        assertEquals(1, (long) jedisRediSearchClient.getKeyCount());

        SearchContext<ProductEntity> searchContext = jedisRediSearchClient.getSearchContextWithFields(Map.of(ARTICLE_NUMBER, product1.getArticleNumber()));
        searchContext.addResultField(ProductEntity.ATTRIBUTES);

        SearchResults<ProductEntity> searchResults = jedisRediSearchClient.find(searchContext);
        assertEquals(1, searchResults.getResults().size());
        assertNotNull(searchResults.getResults().get(0));

        assertEquals(attribute1, searchResults.getResults().get(0).getFieldValue(ATTRIBUTES));
    }
}
