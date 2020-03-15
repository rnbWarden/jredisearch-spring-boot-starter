package com.rnbwarden.redisearch.repository;

import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.client.RediSearchClient;
import com.rnbwarden.redisearch.client.SearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.entity.Brand;
import com.rnbwarden.redisearch.entity.ProductEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Repository
public class ProductRepository {

    @Autowired
    private RediSearchClient<ProductEntity> rediSearchClient;

    public void save(ProductEntity product) {

        rediSearchClient.save(product);
    }

    public void getByKey(String key) {

        rediSearchClient.findByKey(key);
    }

    public void delete(String key) {

        rediSearchClient.delete(key);
    }

    public List<ProductEntity> findByColumn1(String value) {

        SearchResults<ProductEntity> searchResults = rediSearchClient.findByFields(Map.of(ProductEntity.ARTICLE_NUMBER, value));
        //searchResults.getTotalResults();
        return rediSearchClient.deserialize(searchResults);
    }

    public List<ProductEntity> findByColumns(String articleNumber, Brand brand) {

        Map<String, String> fieldNameValues = Map.of(ProductEntity.ARTICLE_NUMBER, articleNumber,
                ProductEntity.BRAND, brand.toString());
        SearchResults<ProductEntity> searchResults = rediSearchClient.findByFields(fieldNameValues);
        //searchResults.getTotalResults();
        return rediSearchClient.deserialize(searchResults);
    }

    public PageableSearchResults<ProductEntity> searchByArticleNumber(String articleNumber) {

        PagingSearchContext ctx = rediSearchClient.getPagingSearchContextWithFields(Map.of(ProductEntity.ARTICLE_NUMBER, articleNumber));
        ctx.setPageSize(100);
        //ctx.setLimit(); setOffset() setNoContent() <-- etc, etc.
        return rediSearchClient.search(ctx);
    }

    public void searchAndConsume(Map<String, String> fieldNameValues, Consumer<ProductEntity> consumer) {

        PagingSearchContext ctx = rediSearchClient.getPagingSearchContextWithFields(fieldNameValues);
        rediSearchClient.search(ctx).resultStream()
                .map(PagedSearchResult::getResult)
                .filter(Optional::isPresent)
                .map(Optional::get)
                //.filter() <-- some additional filter criteria not passed to search
                .forEach(consumer);
    }
}
