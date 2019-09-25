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
                                     long pageSize,
                                     LettuceRediSearchClient<E> lettuceRediSearchClient) {

        this.delegate = delegate;
        this.lettuceRediSearchClient = lettuceRediSearchClient;
        this.iterator = new ResultsIterator(delegate, pageSize);
    }

    @Override
    public Long getTotalResults() {

        return delegate.getCount();
    }

    @Override
    public synchronized Stream<PagedSearchResult<E>> getResultStream(boolean useParallel) {

        return StreamSupport.stream(Spliterators.spliterator(iterator, getTotalResults(),
                Spliterator.NONNULL | Spliterator.CONCURRENT | Spliterator.DISTINCT), useParallel)
                .filter(Objects::nonNull)
                .onClose(iterator::close)
                .map(this::createSearchResult);
    }

    private PagedSearchResult<E> createSearchResult(Map<String, Object> fields) {

        return new LettucePagedCursorSearchResult<>(fields, lettuceRediSearchClient);
    }

    class ResultsIterator implements Iterator<Map<String, Object>>, Closeable {

        private final Object lockObject = new Object();
        private volatile boolean hasNext;
        private volatile ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        private final AtomicLong cursor = new AtomicLong();
        private final long pageSize;

        ResultsIterator(AggregateWithCursorResults<String, Object> delegate, long pageSize) {

            this.pageSize = pageSize;
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

            if (results.peek() != null) {
                return results.poll();
            }
            synchronized (lockObject) {
                populateResultsFromAggregateResults(lettuceRediSearchClient.readCursor(cursor.get(), pageSize));
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
