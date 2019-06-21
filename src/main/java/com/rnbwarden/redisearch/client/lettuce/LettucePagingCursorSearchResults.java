package com.rnbwarden.redisearch.client.lettuce;

import com.redislabs.lettusearch.aggregate.AggregateWithCursorResults;
import com.rnbwarden.redisearch.client.PageableSearchResults;
import com.rnbwarden.redisearch.client.PagedSearchResult;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class LettucePagingCursorSearchResults<E extends RedisSearchableEntity> implements PageableSearchResults<E> {

    private static final Logger LOGGER = Logger.getLogger(LettucePagingCursorSearchResults.class.getName());

    private final AggregateWithCursorResults<String, Object> delegate;
    private final LettuceRediSearchClient<E> lettuceRediSearchClient;
    private final ResultsIterator iterator;

    LettucePagingCursorSearchResults(AggregateWithCursorResults<String, Object> delegate,
                                     LettuceRediSearchClient<E> lettuceRediSearchClient) {

        this.delegate = delegate;
        this.lettuceRediSearchClient = lettuceRediSearchClient;
        this.iterator = new ResultsIterator(delegate);
    }

    @Override
    public Long getTotalResults() {

        return delegate.getCount();
    }

    @Override
    public synchronized Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return StreamSupport.stream(Spliterators.spliterator(iterator, getTotalResults(),
                Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.CONCURRENT | Spliterator.SIZED), useParallel)
                .onClose(iterator::close)
                .filter(Objects::nonNull)
                .map(this::createSearchResult);
    }

    private PagedSearchResult<E> createSearchResult(Map<String, Object> fields) {

        return new LettucePagedCursorSearchResult<>(fields, lettuceRediSearchClient);
    }

    class ResultsIterator implements Iterator<Map<String, Object>>, Closeable {

        private final Object lockObject = new Object();
        private boolean hasNext;
        private ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        private final AtomicLong cursor;

        ResultsIterator(AggregateWithCursorResults<String, Object> delegate) {

            cursor = new AtomicLong();
            populateResultsFromAggregateResults(delegate);
        }

        private void populateResultsFromAggregateResults(AggregateWithCursorResults<String, Object> delegate) {

            ofNullable(delegate).map(AggregateWithCursorResults::getCursor).ifPresent(cursor::set);
            ofNullable(delegate).ifPresent(this.results::addAll);
            hasNext = !results.isEmpty();
        }

        @Override
        public boolean hasNext() {

            return hasNext;
        }

        @Override
        public Map<String, Object> next() {

            Map<String, Object> result = results.poll();
            if (result != null) {
                return result;
            }
            synchronized (lockObject) {
                populateResultsFromAggregateResults(lettuceRediSearchClient.readCursor(cursor.get()));
                hasNext = !results.isEmpty();
            }
            return results.poll();
        }

        @Override
        public void close() {

            try {
                lettuceRediSearchClient.closeCursor(cursor.get());
            } catch (Exception e) {
                LOGGER.warning(() -> "Error closing RediSearch connection. " + e.getMessage());
            }
        }
    }
}
