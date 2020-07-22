package com.rnbwarden.redisearch.client.context;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.function.Consumer;

@Data
@EqualsAndHashCode(callSuper = false)
public class PagingSearchContext<E> extends SearchContext<E> {

    public static final Integer DEFAULT_MAX_LIMIT_VALUE = Integer.MAX_VALUE;

    private boolean useClientSidePaging = false;
    private long pageSize = 1000;
    private Consumer<Exception> exceptionHandler;
}