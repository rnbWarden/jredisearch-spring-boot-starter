package com.rnbwarden.redisearch.client.context;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class PagingSearchContext extends SearchContext {

    private boolean useClientSidePaging = false;
    private long pageSize = 1000;
}