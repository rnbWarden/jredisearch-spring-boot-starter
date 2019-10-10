package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class LettucePagingCursorSearchResults<E extends RedisSearchableEntity> implements PageableSearchResults<E> {

    private final Logger logger = LoggerFactory.getLogger(LettucePagingCursorSearchResults.class.getName());

    private AggregateWithCursorResults<String, Object> delegate;
    private final Supplier<AggregateWithCursorResults<String, Object>> nextPageSupplier;
    private final Function<Map<String, Object>, E> deserializeFunction;
    private final Closeable closeable;
    private final ResultsIterator iterator;

    LettucePagingCursorSearchResults(AggregateWithCursorResults<String, Object> delegate,
                                     Supplier<AggregateWithCursorResults<String, Object>> nextPageSupplier,
                                     Function<Map<String, Object>, E> deserializeFunction,
                                     Closeable closeable) {

        this.nextPageSupplier = nextPageSupplier;
        this.deserializeFunction = deserializeFunction;
        this.closeable = closeable;
        this.iterator = new ResultsIterator(delegate);
    }

    @Override
    public Long getTotalResults() {

        return ofNullable(delegate).map(AggregateWithCursorResults::getCount).orElse(0L);
    }

    @Override
    public synchronized Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return StreamSupport.stream(Spliterators.spliterator(iterator, getTotalResults(),
                Spliterator.NONNULL | Spliterator.CONCURRENT | Spliterator.DISTINCT), useParallel)
                .filter(Objects::nonNull)
                .onClose(this::close)
                .map(this::createSearchResult);
    }

    private void close() {

        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("Error closing PagingCursorSearchResults {}", e.getMessage(), e);
        }
    }

    private PagedSearchResult<E> createSearchResult(Map<String, Object> fields) {

        E entity = deserializeFunction.apply(fields);
        return new LettucePagedCursorSearchResult<>(entity);
    }

    class ResultsIterator implements Iterator<Map<String, Object>> {

        private final Object lockObject = new Object();
        private volatile boolean hasNext;
        private volatile ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();

        ResultsIterator(AggregateWithCursorResults<String, Object> delegate) {

            populateResultsFromAggregateResults(delegate);
        }

        private void populateResultsFromAggregateResults(AggregateWithCursorResults<String, Object> delegate) {

            LettucePagingCursorSearchResults.this.delegate = delegate;
            ofNullable(delegate).ifPresent(this.results::addAll);
            hasNext = !results.isEmpty();
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
                hasNext = !results.isEmpty();
            }
            return results.poll();
        }
    }
}
