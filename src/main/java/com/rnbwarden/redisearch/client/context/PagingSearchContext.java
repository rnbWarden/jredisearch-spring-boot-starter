package com.rnbwarden.redisearch.client.context;

import lombok.Data;

@Data
public class PagingSearchContext extends SearchContext {

    private boolean useClientSidePaging = false;
}