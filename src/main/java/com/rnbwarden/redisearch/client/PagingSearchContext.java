package com.rnbwarden.redisearch.client;

import lombok.Data;

@Data
public class PagingSearchContext extends SearchContext {

    protected Long offset = 0L;
    protected Long limit = 1000000L;
}