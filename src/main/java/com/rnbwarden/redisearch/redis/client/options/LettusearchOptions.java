package com.rnbwarden.redisearch.redis.client.options;

import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import io.redisearch.Query;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

import static io.redisearch.querybuilder.QueryBuilder.intersect;

@Data
//@Builder
public class LettusearchOptions extends RediSearchOptions {

    private Map<String, String> fieldNameValues = new HashMap<>();
    private SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();

    public void addField(String name, String value) {

        fieldNameValues.put(name, value);
    }

    public SearchOptions buildSearchOptions() {

        if (offset != null && limit != null) {
            builder.limit(Limit.builder().num(limit).offset(offset).build());
        }
        return builder.build();
    }
}
