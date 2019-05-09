package com.rnbwarden.redisearch.redis.client.lettuce;

import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.rnbwarden.redisearch.redis.client.RediSearchOptions;
import com.rnbwarden.redisearch.redis.entity.SearchableField;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.redisearch.querybuilder.QueryBuilder.intersect;

@Data
//@Builder
@EqualsAndHashCode(callSuper=false)
public class LettusearchOptions extends RediSearchOptions {

    private Map<SearchableField, String> fieldValues = new HashMap<>();
    private SearchOptions.SearchOptionsBuilder builder = SearchOptions.builder();
    private String queryString;

    public void addField(SearchableField field, String value) {

        fieldValues.put(field, value);
    }

    SearchOptions buildSearchOptions() {

        if (offset != null && limit != null) {
            builder.limit(Limit.builder().num(limit).offset(offset).build());
        }
        return builder.build();
    }

    public String buildQueryString() {

        return fieldValues.entrySet().stream()
                .map(this::getSearchString)
                .collect(Collectors.joining(" ")); //space imply intersection - AND
    }

    private String getSearchString(Map.Entry<SearchableField, String> entry) {

        SearchableField field = entry.getKey();
        return String.format("@%s:%s", field.getName(), field.getQuerySyntax(entry.getValue()));
    }
}
