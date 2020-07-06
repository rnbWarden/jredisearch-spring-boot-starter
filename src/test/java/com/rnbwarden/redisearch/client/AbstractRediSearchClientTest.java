package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.client.lettuce.SearchableLettuceTagField;
import com.rnbwarden.redisearch.entity.RediSearchEntity;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import com.rnbwarden.redisearch.entity.SearchableField;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@RunWith(MockitoJUnitRunner.class)
public class AbstractRediSearchClientTest {
    @Mock
    RedisSerializer<TestingEntity> redisSerializer;


    @Test
    public void buildQueryString() {
        TestingRSCT rsct = new TestingRSCT(TestingEntity.class, redisSerializer, 10000L);
        SearchContext<TestingEntity> searchContext = new SearchContext<>();
        searchContext.addField(new SearchableLettuceTagField<TestingEntity>("Normal", true, testingEntity -> "Normal"), "Normal", false);
        searchContext.addField(new SearchableLettuceTagField<TestingEntity>("Negation", true, testingEntity -> "Negation"), "Negation", true);

        String result = rsct.buildQueryString(searchContext);
        Assert.assertEquals("@Normal:{Normal}-@Negation:{Negation}", result);

    }

    @RediSearchEntity(name = "TestingEntity")
    static class TestingEntity implements RedisSearchableEntity {

        @Override
        public String getPersistenceKey() {
            return "Foo";
        }
    }


    static class TestingRSCT extends AbstractRediSearchClient<TestingEntity, SearchableField<TestingEntity>> {
        protected TestingRSCT(Class<TestingEntity> clazz,
                              RedisSerializer<TestingEntity> redisSerializer,
                              Long defaultMaxResults) {
            super(clazz, redisSerializer, defaultMaxResults);
        }

        @Override
        public void dropIndex() {

        }

        @Override
        public void save(TestingEntity entity) {

        }

        @Override
        public void delete(String key) {

        }

        @Override
        public Optional<TestingEntity> findByKey(String key) {
            return Optional.empty();
        }

        @Override
        public List<TestingEntity> findByKeys(Collection<String> keys) {
            return null;
        }

        @Override
        public SearchResults<TestingEntity> find(SearchContext<TestingEntity> searchContext) {
            return null;
        }

        @Override
        public PageableSearchResults<TestingEntity> search(PagingSearchContext<TestingEntity> pagingSearchContext) {
            return null;
        }

        @Override
        protected void checkAndCreateIndex() {

        }

        @Override
        protected SearchableField<TestingEntity> createSearchableField(RediSearchFieldType type,
                                                            String name,
                                                            boolean sortable,
                                                            Function<TestingEntity, String> serializationFunction) {
            return null;
        }

        @Override
        protected SearchResults<TestingEntity> search(String queryString, SearchContext<TestingEntity> searchContext) {
            return null;
        }

        @Override
        protected PageableSearchResults<TestingEntity> clientSidePagingSearch(String queryString,
                                                                              PagingSearchContext<TestingEntity> pagingSearchContext) {
            return null;
        }

        @Override
        protected PageableSearchResults<TestingEntity> aggregateSearch(String queryString, PagingSearchContext<TestingEntity> searchContext) {
            return null;
        }

    }
}