package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.*;
import com.redislabs.lettusearch.index.CreateOptions;
import com.redislabs.lettusearch.index.DropOptions;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.field.FieldOptions;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.*;
import com.rnbwarden.redisearch.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.entity.RediSearchFieldType;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.redislabs.lettusearch.search.Direction.Ascending;
import static com.redislabs.lettusearch.search.Direction.Descending;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

@Slf4j
public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private com.redislabs.lettusearch.RediSearchClient rediSearchClient;
    private final Supplier<StatefulRediSearchConnection<String, Object>> connectionSupplier;
    private final GenericObjectPool<StatefulRediSearchConnection<String, Object>> pool;

    public LettuceRediSearchClient(Class<E> clazz,
                                   com.redislabs.lettusearch.RediSearchClient rediSearchClient,
                                   RedisCodec<String, Object> redisCodec,
                                   RedisSerializer<E> redisSerializer,
                                   Long defaultMaxResults) {

        super(clazz, redisSerializer, defaultMaxResults);
        this.rediSearchClient = rediSearchClient;
        this.connectionSupplier = () -> rediSearchClient.connect(redisCodec);
        this.pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier, new GenericObjectPoolConfig<>());
        checkAndCreateIndex();
    }

    @Override
    protected void checkAndCreateIndex() {

        StatefulRediSearchConnection<String, String> uncompressedConnection = null;
        try {
            uncompressedConnection = rediSearchClient.connect();
            uncompressedConnection.sync().ftInfo(index);
            alterSchema(uncompressedConnection);
        } catch (RedisCommandExecutionException ex) {
            if (uncompressedConnection == null) {
                throw ex;
            }
            if (ex.getCause().getMessage().equals("Unknown Index name")) {
                uncompressedConnection.sync().create(index, createSchema(), CreateOptions.builder().build());
            }
        } finally {
            if (uncompressedConnection != null) {
                uncompressedConnection.close();
            }
        }
    }

    /**
     * Alter the existing schema to add any missing fields
     */
    private void alterSchema(StatefulRediSearchConnection<String, String> connection) {

        logger.info("checking for new fields for existing ReidSearch schema for index: " + index);
        getFields().stream()
                .map(SearchableLettuceField::getField)
                .forEach(field -> {
                    try {
                        connection.sync().alter(index, field.getName(), FieldOptions.builder().sortable(field.isSortable()).build());
                    } catch (RedisCommandExecutionException e) {
                        if (!e.getMessage().equalsIgnoreCase("Duplicate field in schema")) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    private Schema createSchema() {

        Schema.SchemaBuilder builder = Schema.builder();
        getFields().stream()
                .map(SearchableLettuceField::getField)
                .forEach(builder::field);
        return builder.build();
    }

    @Override
    protected SearchableLettuceField<E> createSearchableField(RediSearchFieldType type,
                                                              String name,
                                                              boolean sortable,
                                                              Function<E, String> serializationFunction) {

        if (SERIALIZED_DOCUMENT.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException(format("Field name '%s' is not protected! Please use another name.", name));
        }
        if (type == RediSearchFieldType.TEXT) {
            return new SearchableLettuceTextField<>(name, sortable, serializationFunction);
        }
        if (type == RediSearchFieldType.TAG) {
            return new SearchableLettuceTagField<>(name, sortable, serializationFunction);
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
    public Long getKeyCount() {

        return getKeyCount(ALL_QUERY, 10000L);
    }

    @Override
    public Long getKeyCount(PagingSearchContext<E> pagingSearchContext) {

        String queryString = buildQueryString(pagingSearchContext);
        return getKeyCount(queryString, pagingSearchContext.getPageSize());
    }

    private Long getKeyCount(String queryString, long pageSize) {

        AggregateOptions aggregateOptions = AggregateOptions.builder().build();
        Cursor cursor = Cursor.builder().count(pageSize).build();
        long count;
        try (StatefulRediSearchConnection<String, Object> connection = connectionSupplier.get()) {
            AggregateWithCursorResults<String, Object> aggregateResults = connection.sync().aggregate(index, queryString, cursor, aggregateOptions);
            count = aggregateResults.size();
            while (aggregateResults.getCursor() != 0) {
                aggregateResults = readCursor(aggregateResults.getCursor(), pageSize, connection);
                count += aggregateResults.size();
            }
        }
        return count;
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        Document<String, Object> document = Document.<String, Object>builder().id(key).fields(fields).build();
        execute(connection -> {
            return connection.sync().add(index, document, AddOptions.builder().replace(true).build());
        });
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
    public List<E> findByKeys(Collection<String> keys) {

        return performTimedOperation("findByKeys",
                () -> {
                    String[] qualifiedKeys = keys.stream().map(this::getQualifiedKey).toArray(String[]::new);
                    return getByKeys(qualifiedKeys).stream()
                            .filter(Objects::nonNull)
                            .map(map -> map.get(SERIALIZED_DOCUMENT))
                            .map(byte[].class::cast)
                            .map(redisSerializer::deserialize)
                            .collect(Collectors.toList());
                });
    }

    private List<Map<String, Object>> getByKeys(String[] qualifiedKeys) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return connection.sync().ftMget(index, qualifiedKeys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public com.rnbwarden.redisearch.client.SearchResults<E> find(SearchContext<E> searchContext) {

        return performTimedOperation("search", () -> search(buildQueryString(searchContext), searchContext));
    }

    @Override
    protected com.rnbwarden.redisearch.client.SearchResults<E> search(String queryString, SearchContext<E> searchContext) {

        return execute(connection -> {
            SearchOptions searchOptions = configureQueryOptions(searchContext);
            com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, searchOptions);
            logger.debug("found count {}", searchResults.getCount());
            return new LettuceSearchResults<>(keyPrefix, searchResults);
        });
    }

    private SearchOptions configureQueryOptions(SearchContext<E> searchContext) {

        SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
        Optional.ofNullable(searchContext.getSortBy())
                .ifPresent(sortBy -> builder.sortBy(SortBy.builder().field(sortBy)
                        .direction(searchContext.isSortAscending() ? Ascending : Descending).build()));
        builder.limit(Limit.builder().num(searchContext.getLimit()).offset(searchContext.getOffset()).build());
        builder.noContent(searchContext.isNoContent());
        return builder.build();
    }

    @Override
    public PageableSearchResults<E> search(PagingSearchContext<E> pagingSearchContext) {

        return performTimedOperation("search", () -> pagingSearch(buildQueryString(pagingSearchContext), pagingSearchContext));
    }

    @Override
    protected PageableSearchResults<E> clientSidePagingSearch(String queryString, PagingSearchContext<E> pagingSearchContext) {

        pagingSearchContext.setNoContent(true); //First query should explicitly avoid retrieving data
        return execute(connection -> {
            SearchOptions lettusearchOptions = configureQueryOptions(pagingSearchContext);
            SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, lettusearchOptions);
            logger.debug("found count {}", searchResults.getCount());
            return new LettucePagingSearchResults<>(keyPrefix, searchResults, this, pagingSearchContext.getExceptionHandler());
        });
    }

    @Override
    protected PageableSearchResults<E> aggregateSearch(String queryString, PagingSearchContext<E> searchContext) {

        AggregateOptions.AggregateOptionsBuilder aggregateOptionsBuilder = AggregateOptions.builder()
                .operation(com.redislabs.lettusearch.aggregate.Limit.builder().num(searchContext.getLimit())
                        .offset(searchContext.getOffset()).build());

        ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> {
            SortProperty sortProperty = SortProperty.builder().property(sortBy).order(searchContext.isSortAscending() ? Order.Asc : Order.Desc).build();
            aggregateOptionsBuilder.operation(Sort.builder().property(sortProperty).build());
        });
        aggregateOptionsBuilder.load(SERIALIZED_DOCUMENT);

        AggregateOptions aggregateOptions = aggregateOptionsBuilder.build();
        long pageSize = searchContext.getPageSize();
        Cursor cursor = Cursor.builder().count(pageSize).build();

        StatefulRediSearchConnection<String, Object> connection = connectionSupplier.get();
        try {
            AggregateWithCursorResults<String, Object> aggregateResults = connection.sync().aggregate(index, queryString, cursor, aggregateOptions);
            return new LettucePagingCursorSearchResults<>(aggregateResults,
                    () -> readCursor(aggregateResults.getCursor(), pageSize, connection),
                    this::deserialize,
                    () -> closeCursor(connection, aggregateResults.getCursor()),
                    searchContext.getExceptionHandler());
        } catch (Exception e) {
            close(connection);
            throw (e);
        }
    }

    private <R> R execute(Function<StatefulRediSearchConnection<String, Object>, R> function) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return function.apply(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private AggregateWithCursorResults<String, Object> readCursor(Long cursor, Long count, StatefulRediSearchConnection<String, Object> connection) {

        if (cursor == 0) {
            close(connection);
            return null;
        }
        try {
            return connection.sync().cursorRead(index, cursor, count);
        } catch (RedisCommandExecutionException redisCommandExecutionException) {
            closeCursor(connection, cursor);
            if ("Cursor not found".equalsIgnoreCase(redisCommandExecutionException.getMessage())) {
                return null;
            }
            throw (redisCommandExecutionException);
        }
    }

    private void closeCursor(StatefulRediSearchConnection<String, Object> connection, Long cursor) {

        if (cursor != null) {
            try {
                connection.async().cursorDelete(index, cursor);
            } catch (Exception e) {
                logger.warn("Error closing RediSearch cursor. " + e.getMessage(), e);
            }
        }
        close(connection);
    }

    private void close(StatefulRediSearchConnection<String, Object> connection) {

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing RediSearch connection. " + e.getMessage(), e);
        }
    }
}