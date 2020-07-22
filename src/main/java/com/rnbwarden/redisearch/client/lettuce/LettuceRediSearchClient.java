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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.redislabs.lettusearch.search.Direction.Ascending;
import static com.redislabs.lettusearch.search.Direction.Descending;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

@Slf4j
public class LettuceRediSearchClient<E extends RedisSearchableEntity> extends AbstractRediSearchClient<E, SearchableLettuceField<E>> {

    private final Logger logger = LoggerFactory.getLogger(LettuceRediSearchClient.class);
    private final com.redislabs.lettusearch.RediSearchClient rediSearchClient;
    private final GenericObjectPool<StatefulRediSearchConnection<String, Object>> pool;

    public LettuceRediSearchClient(Class<E> clazz,
                                   RedisSerializer<E> redisSerializer,
                                   com.redislabs.lettusearch.RediSearchClient rediSearchClient,
                                   GenericObjectPool<StatefulRediSearchConnection<String, Object>> pool) {

        super(clazz, redisSerializer);
        this.rediSearchClient = rediSearchClient;
        this.pool = pool;
        checkAndCreateIndex();
    }

    @Override
    protected void checkAndCreateIndex() {

        try {
            logger.info("checking RediSearch index: {}", index);
            getFtInfo();
            alterSchema();
        } catch (RedisCommandExecutionException ex) {
            if (ex.getCause().getMessage().equals("Unknown Index name")) {
                execute(connection -> connection.sync().create(index, createSchema(), CreateOptions.builder().build()));
            }
        }
    }

    /**
     * Alter the existing schema to add any missing fields
     */
    private void alterSchema() {

        logger.info("checking for new fields & options for existing ReidSearch schema for index: {}", index);
        getFields().stream()
                .filter(SearchableLettuceField::isSearchable)
                .map(SearchableLettuceField::getField)
                .forEach(field -> {
                    try (StatefulRediSearchConnection<String, String> connection = rediSearchClient.connect()) {
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
                .filter(SearchableLettuceField::isSearchable)
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
        if (type == RediSearchFieldType.NO_INDEX) {
            return new NonSearchableLettuceField<>(name, sortable, serializationFunction);
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

        Map<String, Object> ftInfoMap = getFtInfo();
        return Long.valueOf((String)ftInfoMap.get("num_docs"));
    }

    public Map<String, Object> getFtInfo() {

        Map<String, Object> ftInfoMap = new HashMap<>();
        try (StatefulRediSearchConnection<String, String> connection = rediSearchClient.connect()) {
            Iterator<Object> ftInfoIter = connection.sync().ftInfo(index).iterator();
            while (ftInfoIter.hasNext()) {
                ftInfoMap.put((String)ftInfoIter.next(), ftInfoIter.next());
            }
        }
        return ftInfoMap;
    }

    @Override
    public Long getKeyCount(PagingSearchContext<E> pagingSearchContext) {

        String queryString = buildQueryString(pagingSearchContext);
        AggregateOptions aggregateOptions = AggregateOptions.builder().build();
        Cursor cursor = Cursor.builder().count(pagingSearchContext.getPageSize()).build();
        AggregateWithCursorResults<String, Object> aggregateResults =  execute(connection -> connection.sync().aggregate(index, queryString, cursor, aggregateOptions));
        long count = aggregateResults.size();
        while (aggregateResults.getCursor() != 0) {
            aggregateResults = readCursor(aggregateResults.getCursor(), pagingSearchContext.getPageSize());
            count += aggregateResults.size();
        }
        return count;
    }

    @Override
    public void save(E entity) {

        Map<String, Object> fields = serialize(entity);
        String key = getQualifiedKey(entity.getPersistenceKey());
        Document<String, Object> document = Document.<String, Object>builder().id(key).fields(fields).build();
        execute(connection -> connection.sync().add(index, document, AddOptions.builder().replace(true).build()));
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

        return ofNullable(getByKey(key))
                .map(map -> map.get(SERIALIZED_DOCUMENT))
                .map(byte[].class::cast)
                .map(redisSerializer::deserialize);
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

        String[] qualifiedKeys = keys.stream().map(this::getQualifiedKey).toArray(String[]::new);
        return getByKeys(qualifiedKeys).stream()
                .filter(Objects::nonNull)
                .map(map -> map.get(SERIALIZED_DOCUMENT))
                .map(byte[].class::cast)
                .map(redisSerializer::deserialize)
                .collect(Collectors.toList());
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

        return search(buildQueryString(searchContext), searchContext);
    }

    @Override
    protected com.rnbwarden.redisearch.client.SearchResults<E> search(String queryString,
                                                                      SearchContext<E> searchContext) {

        return execute(connection -> {
            SearchOptions searchOptions = configureQueryOptions(searchContext);
            com.redislabs.lettusearch.search.SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, searchOptions);
            logger.debug("found count {}", searchResults.getCount());
            return new LettuceSearchResults<>(keyPrefix, searchResults);
        });
    }

    private SearchOptions configureQueryOptions(SearchContext<E> searchContext) {

        SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
        builder.noContent(searchContext.isNoContent());
        Optional.ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> builder.sortBy(SortBy.builder().field(sortBy).direction(searchContext.isSortAscending() ? Ascending : Descending).build()));
        builder.limit(Limit.builder().num(ofNullable(searchContext.getLimit()).orElse(SearchContext.DEFAULT_MAX_LIMIT_VALUE)).offset(searchContext.getOffset()).build());
        Optional.ofNullable(searchContext.getResultFields()).ifPresent(builder::returnFields);
        return builder.build();
    }

    @Override
    public PageableSearchResults<E> search(PagingSearchContext<E> pagingSearchContext) {

        return pagingSearch(buildQueryString(pagingSearchContext), pagingSearchContext);
    }

    @Override
    protected PageableSearchResults<E> clientSidePagingSearch(String queryString,
                                                              PagingSearchContext<E> pagingSearchContext) {

        return execute(connection -> {
            pagingSearchContext.setNoContent(true); //First query should explicitly avoid retrieving data
            SearchOptions searchOptions = configureQueryOptions(pagingSearchContext);
            SearchResults<String, Object> searchResults = connection.sync().search(index, queryString, searchOptions);
            return new LettucePagingSearchResults<>(keyPrefix, searchResults, this, pagingSearchContext.getExceptionHandler());
        });
    }

    @Override
    protected PageableSearchResults<E> aggregateSearch(String queryString, PagingSearchContext<E> searchContext) {

        AggregateOptions.AggregateOptionsBuilder aggregateOptionsBuilder = AggregateOptions.builder()
                .operation(com.redislabs.lettusearch.aggregate.Limit.builder()
                        .num(ofNullable(searchContext.getLimit()).orElse(PagingSearchContext.DEFAULT_MAX_LIMIT_VALUE))
                        .offset(searchContext.getOffset()).build());

        ofNullable(searchContext.getSortBy()).ifPresent(sortBy -> {
            SortProperty sortProperty = SortProperty.builder().property(sortBy).order(searchContext.isSortAscending() ? Order.Asc : Order.Desc).build();
            aggregateOptionsBuilder.operation(Sort.builder().property(sortProperty).build());
        });
        aggregateOptionsBuilder.load(SERIALIZED_DOCUMENT);

        AggregateOptions aggregateOptions = aggregateOptionsBuilder.build();
        long pageSize = searchContext.getPageSize();
        Cursor cursor = Cursor.builder().count(pageSize).build();

        return execute(connection -> {
            AggregateWithCursorResults<String, Object> aggregateResults = connection.sync().aggregate(index, queryString, cursor, aggregateOptions);
            return new LettucePagingCursorSearchResults<>(aggregateResults,
                    () -> readCursor(aggregateResults.getCursor(), pageSize),
                    this::deserialize,
                    () -> closeCursor(aggregateResults.getCursor()),
                    searchContext.getExceptionHandler());
        });
    }

    private <R> R execute(Function<StatefulRediSearchConnection<String, Object>, R> function) {

        try (StatefulRediSearchConnection<String, Object> connection = pool.borrowObject()) {
            return function.apply(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private AggregateWithCursorResults<String, Object> readCursor(Long cursor, Long count) {

        if (cursor == 0) {
            return null;
        }
        return execute(connection -> {
            try {
                return connection.sync().cursorRead(index, cursor, count);
            } catch (RedisCommandExecutionException redisCommandExecutionException) {
                closeCursor(cursor);
                if ("Cursor not found".equalsIgnoreCase(redisCommandExecutionException.getMessage())) {
                    return null;
                }
                throw (redisCommandExecutionException);
            }
        });
    }

    private void closeCursor(Long cursor) {

        if (cursor != null) {
            try {
                execute(connection -> connection.async().cursorDelete(index, cursor));
            } catch (Exception e) {
                logger.warn("Cannot close RediSearch cursor. " + e.getMessage(), e);
            }
        }
    }
}