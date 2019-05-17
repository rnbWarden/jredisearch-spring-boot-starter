package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.SearchResult;
import io.redisearch.Document;

import java.util.HashMap;
import java.util.Map;

public class JedisSearchResult implements SearchResult<String, Object> {

    private final Document delegate;
    private final Map<String, Object> properties = new HashMap<>();
    private final String keyPrefix;

    JedisSearchResult(String keyPrefix, Document delegate) {

        this.delegate = delegate;
        this.keyPrefix = keyPrefix;
        delegate.getProperties().forEach(entry -> properties.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public Map<String, Object> getFields() {

        return properties;
    }

    @Override
    public Object getField(String key) {

        return properties.get(key);
    }

    @Override
    public String getId() {

        String id = delegate.getId();
        return id.substring(keyPrefix.length());
    }
}