package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.SearchResult;
import io.redisearch.Document;

import java.util.HashMap;
import java.util.Map;

public class JedisSearchResult implements SearchResult<String, Object> {

    private final Document delegate;
    private final Map<String, Object> properties = new HashMap<>();

    JedisSearchResult(Document delegate) {

        this.delegate = delegate;
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

        return delegate.getId();
    }
}