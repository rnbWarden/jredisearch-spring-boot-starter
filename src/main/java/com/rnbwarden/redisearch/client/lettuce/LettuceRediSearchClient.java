package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.*;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.*;
import com.redislabs.lettusearch.search.field.FieldOptions;
import com.rnbwarden.redisearch.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.SearchContext;
import com.rnbwarden.redisearch.entity.QueryField;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.redislabs.lettusearch.aggregate.Limit.builder;
import static com.redislabs.lettusearch.search.SortBy.Direction.Ascending;
import static com.redislabs.lettusearch.search.SortBy.Direction.Descending;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private com.redislabs.lettusearch.RediSearchClient rediSearchClient;
    private final GenericObjectPool<StatefulRediSearchConnection<String, Object>> pool;

    public LettuceRediSearchClient(Class<E> clazz,
                                   com.redislabs.lettusearch.RediSearchClient rediSearchClient,
                                   RedisCodec<String, Object> redisCodec,
                                   RedisSerializer<E> redisSerializer,
                                   Long defaultMaxResults) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.rediSearchClient = rediSearchClient;
        pool = ConnectionPoolSupport.createGenericObjectPool(() -> rediSearchClient.connect(redisCodec), new GenericObjectPoolConfig());
        checkAndCreateIndex();
    }

    @Override
    protected void checkAndCreateIndex() {

        StatefulRediSearchConnection<String, String> uncompressedConnection = null;
        try {
            uncompressedConnection = rediSearchClient.connect();
            uncompressedConnection.sync().indexInfo(index);
            alterSortableFields(uncompressedConnection);
        } catch (RedisCommandExecutionException ex) {
            if (uncompressedConnection == null) {
                throw ex;
            }
            if (ex.getCause().getMessage().equals("Unknown Index name")) {
                uncompressedConnection.sync().create(index, createSchema());
            }
        } finally {
            if (uncompressedConnection != null) {
                uncompressedConnection.close();
            }
        }
    }

    /**
     * This is a patch for any existing indexes created before search capability was added to the starter
     */
    private void alterSortableFields(StatefulRediSearchConnection<String, String> connection) {

        getFields().stream()
                .map(SearchableLettuceField::getField)
                .filter(com.redislabs.lettusearch.search.field.Field::isSortable)
                .map(com.redislabs.lettusearch.search.field.Field::getName)
                .forEach(fieldName -> connection.sync().alter(index, fieldName, FieldOptions.builder().sortable(true).build()));
    }

    private Schema createSchema() {

        Schema.SchemaBuilder builder = Schema.builder();
        getFields().stream()
                .map(SearchableLettuceField::getField)
                .forEach(builder::field);
        return builder.build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SearchableLettuceField<E> createSearchableField(RediSearchFieldType type,
                                                              String name,
                                                              boolean sortable,
                                                              Function<E, String> serializationFunction) {

        if (type == RediSearchFieldType.TEXT) {
            return new SearchableLettuceTextField(name, sortable, serializationFunction);
        }
        if (type == RediSearchFieldType.TAG) {
            return new SearchableLettuceTagField(name, sortable, serializationFunction);
        }
        throw new IllegalArgumentException(format("field type '%s' is not supported", type));
    }

    @Override
    public void dropIndex() {

        try (StatefulRediSearchConnection<String, String> uncompressedConnection = rediSearchClient.connect()) {
            uncompressedConnection.sync().drop(index, DropOptions.builder().keepDocs(false).build());
        }
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        execute(connection -> connection.sync().add(index, key, 1, fields, AddOptions.builder().replace(true).build()));
    }

    @Override
    public void delete(String key) {

        execute(connection -> connection.sync().del(index, getQualifiedKey(key), true));
    }

    @Override
    public Optional<E> findByKey(String key) {

        return findByQualifiedKey(getQualifiedKey(key));
    }

    Optional<E> findByQualifiedKey(String key) {

        return performTimedOperation("findByKey",
                () -> ofNullable(getByKey(key))
                        .map(map -> map.get(SERIALIZED_DOCUMENT))
                        .map(byte[].class::cast)
                        .map(redisSerializer::deserialize));
    }

    private Map<String, Object> getByKey(String key) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return connection.sync().get(index, key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public com.rnbwarden.redisearch.client.SearchResults<E> find(SearchContext searchContext) {

        return performTimedOperation("search", () -> search(buildQueryString(searchContext), searchContext));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected com.rnbwarden.redisearch.client.SearchResults<E> search(String queryString, SearchContext searchContext) {

        return execute(connection -> {
            com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, configureQueryOptions(searchContext));
            logger.debug("found count {}", searchResults.getCount());
            return new LettuceSearchResults(keyPrefix, searchResults);
        });
    }

    private String buildQueryString(SearchContext searchContext) {

        List<QueryField> queryFields = searchContext.getQueryFields();
        StringBuilder sb = new StringBuilder();
        queryFields.stream()
                .map(queryField -> format("@%s:%s", queryField.getName(), queryField.getQuerySyntax()))
                .forEach(sb::append);
        return sb.toString();
    }

    private SearchOptions configureQueryOptions(SearchContext searchContext) {

        SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
        String sortBy = searchContext.getSortBy();
        if (sortBy != null) {
            builder.sortBy(SortBy.builder().field(sortBy).direction(searchContext.isSortAscending() ? Ascending : Descending).build());
        }
        builder.limit(Limit.builder().num(defaultMaxResults).offset(0).build());
        builder.noContent(searchContext.isNoContent());
        return builder.build();
    }

    @Override
    public PageableSearchResults<E> search(SearchContext pageableContent) {

        return performTimedOperation("search", () -> pagingSearch(buildQueryString(pageableContent), pageableContent));
    }

    @Override
    public PageableSearchResults<E> findAll(Integer limit) {

        SearchContext context = new SearchContext();
        context.setLimit(Long.valueOf(ofNullable(limit).orElse(defaultMaxResults.intValue())));
        context.setUseClientSidePaging(false);
        return findAll(context);
    }

    @Override
    protected PageableSearchResults<E> pagingSearch(String queryString, SearchContext searchContext) {

        assert (searchContext != null);
        assert (searchContext.getOffset() != null);
        assert (searchContext.getLimit() != null);

        if (!searchContext.isUseClientSidePaging()) {
            return aggregateSearch(queryString, searchContext);
        }

        searchContext.setNoContent(true);

        return execute(connection -> {
            SearchOptions searchOptions = configureQueryOptions(searchContext);
            searchOptions.setLimit(Limit.builder().num(searchContext.getLimit()).offset(searchContext.getOffset()).build());

            com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, searchOptions);
            logger.debug("found count {}", searchResults.getCount());
            return new LettucePagingSearchResults<>(searchResults, this);
        });
    }

    private PageableSearchResults<E> aggregateSearch(String queryString, SearchContext searchContext) {

        com.redislabs.lettusearch.aggregate.Limit limit = builder().num(searchContext.getLimit()).offset(searchContext.getOffset()).build();

        AggregateOptions.AggregateOptionsBuilder aggregateOptionsBuilder = AggregateOptions.builder().operation(limit);

        ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> {
            SortProperty sortProperty = SortProperty.builder().property(sortBy).order(searchContext.isSortAscending() ? Order.Asc : Order.Desc).build();
            aggregateOptionsBuilder.operation(Sort.builder().property(sortProperty).build());
        });
//        getAllFieldNames().forEach(aggregateOptionsBuilder::load);
        aggregateOptionsBuilder.load(SERIALIZED_DOCUMENT);
        AggregateOptions aggregateOptions = aggregateOptionsBuilder.build();

        CursorOptions cursorOptions = CursorOptions.builder().maxIdle(30000L).build();

        return execute(connection -> {
            AggregateWithCursorResults<String, Object> aggregateResults = connection.sync().aggregate(index, queryString, aggregateOptions, cursorOptions);
            return new LettucePagingCursorSearchResults<>(aggregateResults, this);
        });
    }

    private <R extends Object> R execute(Function<StatefulRediSearchConnection<String, Object>, R> function) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return function.apply(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    AggregateWithCursorResults<String, Object> readCursor(Long cursor) {

        return execute(connection -> {
            try {
                return connection.sync().cursorRead(index, cursor);
            } catch (RedisCommandExecutionException redisCommandExecutionException) {
                if ("Cursor not found".equalsIgnoreCase(redisCommandExecutionException.getMessage())) {
                    return null;
                }
                throw (redisCommandExecutionException);
            }
        });
    }

    void closeCursor(long cursor) {

        execute(connection -> connection.async().cursorDelete(index, cursor));
    }
}