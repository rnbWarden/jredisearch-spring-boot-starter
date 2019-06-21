package com.rnbwarden.redisearch.client;

import lombok.Data;

@Data
public class PagingSearchContext extends SearchContext {

    private Long offset = 0L;
    private Long limit = 1000000L;
    private boolean useClientSidePaging = true;
}