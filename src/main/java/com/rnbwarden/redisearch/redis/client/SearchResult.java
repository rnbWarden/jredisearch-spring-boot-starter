package com.rnbwarden.redisearch.redis.client;

import java.util.List;

public interface SearchResult {

    Long getTotalResults();
    List<Object> getFieldsByKey(String key);

}
