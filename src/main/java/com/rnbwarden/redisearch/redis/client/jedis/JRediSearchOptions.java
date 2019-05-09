package com.rnbwarden.redisearch.redis.client.jedis;

import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import io.redisearch.Query;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;

import static io.redisearch.querybuilder.QueryBuilder.intersect;

@Data
//@Builder
@EqualsAndHashCode(callSuper=false)
public class JRediSearchOptions extends RediSearchOptions {

    private Map<String, String> fieldNameValues = new HashMap<>();
    private Query query = new Query(intersect().toString());

    public void addField(String name, String value) {

        fieldNameValues.put(name, value);
    }

    public Query buildQuery() {

        if (offset != null && limit != null) {
            query.limit(offset.intValue(), limit.intValue());
        }
        return query;
    }
}
