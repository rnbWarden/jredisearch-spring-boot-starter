package com.rnbwarden.redisearch.redis.client;

import lombok.Data;

@Data
public abstract class RediSearchOptions {

    static long defaultMaxValue = Long.MAX_VALUE;

    protected boolean noContent;
    protected boolean verbatim;
    protected boolean noStopWords;
    protected boolean withScores;
    protected boolean withPayloads;
    protected boolean withSortKeys;
    protected String language;
    protected String sortBy;
    protected Long offset;
    protected Long limit;
}