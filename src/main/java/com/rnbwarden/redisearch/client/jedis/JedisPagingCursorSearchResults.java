package com.rnbwarden.redisearch.client.jedis;

import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.redisearch.AggregationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class JedisPagingCursorSearchResults<E extends RedisSearchableEntity> implements PageableSearchResults<E> {

    private final Logger logger = LoggerFactory.getLogger(JedisPagingCursorSearchResults.class.getName());

    private AggregationResult delegate;
    private final Supplier<AggregationResult> nextPageSupplier;
    private final Function<Map<String, Object>, E> deserializeFunction;
    private final Closeable closeable;
    private final ResultsIterator iterator;
    private final Consumer<Exception> exceptionConsumer;

    JedisPagingCursorSearchResults(AggregationResult delegate,
                                   Supplier<AggregationResult> nextPageSupplier,
                                   Function<Map<String, Object>, E> deserializeFunction,
                                   Closeable closeable,
                                   Consumer<Exception> exceptionConsumer) {

        this.nextPageSupplier = nextPageSupplier;
        this.deserializeFunction = deserializeFunction;
        this.closeable = closeable;
        this.iterator = new ResultsIterator(delegate);
        this.exceptionConsumer = exceptionConsumer;
    }

    @Override
    public Long getTotalResults() {

        return ofNullable(delegate).map(aggregationResult -> aggregationResult.totalResults).orElse(0L);
    }

    @Override
    public Stream<PagedSearchResult<E>> resultStream() {

        return getResultStream(false);
    }

    @Override
    public Stream<PagedSearchResult<E>> parallelStream() {

        return getResultStream(true);
    }

    @Override
    public synchronized Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return StreamSupport.stream(Spliterators.spliterator(iterator, getTotalResults(),
                Spliterator.NONNULL | Spliterator.CONCURRENT | Spliterator.DISTINCT), useParallel)
                .filter(Objects::nonNull)
                .onClose(this::close)
                .map(this::createSearchResult)
                .filter(Objects::nonNull);
    }

    public void close() {

        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing PagingCursorSearchResults {}", e.getMessage(), e);
        }
    }

    private PagedSearchResult<E> createSearchResult(Map<String, Object> fields) {

        try {

            E entity = deserializeFunction.apply(fields);
            return new JedisPagedCursorSearchResult<>(entity);
        } catch (Exception e) {
            if (exceptionConsumer != null) {
                exceptionConsumer.accept(e);
                return null;
            }
            throw e;
        }
    }

    class ResultsIterator implements Iterator<Map<String, Object>> {

        private final Object lockObject = new Object();
        private final ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        private volatile boolean hasNext = true;

        ResultsIterator(AggregationResult delegate) {

            populateResultsFromAggregateResults(delegate);
        }

        private void populateResultsFromAggregateResults(AggregationResult delegate) {

            JedisPagingCursorSearchResults.this.delegate = delegate;
            ofNullable(delegate).map(AggregationResult::getResults).ifPresent(this.results::addAll);
            if (results.isEmpty()) {
                hasNext = false;
                close();
            }
        }

        @Override
        public boolean hasNext() {

            return hasNext;
        }

        @Override
        public Map<String, Object> next() {

            if (results.peek() != null) {
                return results.poll();
            }
            synchronized (lockObject) {
                populateResultsFromAggregateResults(nextPageSupplier.get());
            }
            return results.poll();
        }
    }
}
