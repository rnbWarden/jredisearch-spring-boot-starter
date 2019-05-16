package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.entity.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.StopWatch;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public abstract class AbstractRediSearchClient<E extends RedisSearchableEntity, T extends SearchableField<E>> implements RediSearchClient<E> {

    protected static final String SERIALIZED_DOCUMENT = "sdoc";
    protected static final String ALL_QUERY = "*";

    private final Logger logger = LoggerFactory.getLogger(AbstractRediSearchClient.class);
    private final Long defaultMaxResults;
    protected final String index;
    protected final String keyPrefix;

    protected final RedisSerializer<E> redisSerializer;
    protected final Class<E> clazz;
    private final List<T> fields = new ArrayList<>();

    protected AbstractRediSearchClient(CompressingJacksonSerializer<E> redisSerializer,
                                       Long defaultMaxResults) {

        this.redisSerializer = redisSerializer;
        this.defaultMaxResults = defaultMaxResults;
        this.clazz = redisSerializer.getClazz();
        this.index = getIndex(clazz);
        this.keyPrefix = format("%s:", index);
        initSearchableFields();
    }

    private void initSearchableFields() {

        fields.addAll(getSearchableFieldsFromFields());
        fields.addAll(getSearchFieldsFromMethods());
    }

    protected abstract Map<RediSearchFieldType, BiFunction<String, Function<E, String>, T>> getFieldStrategy();

    protected abstract void checkAndCreateIndex();

    private List<T> getSearchableFieldsFromFields() {

        return FieldUtils.getFieldsListWithAnnotation(clazz, RediSearchField.class).stream()
                .map(field -> stream(field.getAnnotations())
                        .filter(annotation -> annotation instanceof RediSearchField)
                        .map(RediSearchField.class::cast)
                        .map(annotation -> {
                            RediSearchFieldType type = annotation.type();
                            String name = annotation.name();
                            return getFieldStrategy().get(type).apply(name, e -> getFieldValue(field, e));
                        })
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(toList());
    }

    private List<T> getSearchFieldsFromMethods() {

        return MethodUtils.getMethodsListWithAnnotation(clazz, RediSearchField.class).stream()
                .map(method -> stream(method.getAnnotations())
                        .filter(annotation -> annotation instanceof RediSearchField)
                        .map(RediSearchField.class::cast)
                        .map(annotation -> getFieldStrategy().get(annotation.type()).apply(annotation.name(), e -> {
                            try {
                                Object o = method.invoke(e, (Object[]) null);
                                return getSerializedObjectValue(o);
                            } catch (Exception ex) {
                                throw new RuntimeException(String.format("cannot invoke method:%s on %s", method.getName(), e.getClass()), ex);
                            }
                        }))
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(toList());
    }

    private String getFieldValue(Field f, Object obj) {

        try {
            boolean accessible = f.isAccessible();

            f.setAccessible(true);
            Object o = f.get(obj);
            f.setAccessible(accessible);

            return getSerializedObjectValue(o);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(format("Unable to get RediSearch annotated entity value for entity: %s of class: %s", f.getName(), obj.getClass()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private String getSerializedObjectValue(Object o) {

        if (o == null) {
            return null;
        }
        if (!Collection.class.isAssignableFrom(o.getClass())) {
            return o.toString();
        }
        return (String) ((Collection) o).stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .collect(joining(","));
    }

    protected List<T> getFields() {

        return fields;
    }

    protected SearchableField<E> getField(String name) {

        return fields.stream()
                .filter(f -> f.getName().equals(name))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("invalid entity name: " + name));
    }

    protected Map<String, Object> serialize(E entity) {

        Map<String, Object> fields = new HashMap<>();
        getFields().forEach(field -> {
            String serializedValue = field.serialize(entity);
            if (serializedValue != null) {
                fields.put(field.getName(), serializedValue);
            }
        });
        fields.put(SERIALIZED_DOCUMENT, redisSerializer.serialize(entity));
        return fields;
    }

    @Override
    public void recreateIndex() {

        dropIndex();
        checkAndCreateIndex();
    }

    @Override
    public Long getKeyCount() {

        return findAll(0, 0, false).getTotalResults();
    }

    @Override
    public List<E> deserialize(SearchResults searchResults) {

        return ofNullable(searchResults)
                .map(SearchResults::getResults)
                .map(results -> results.stream()
                        .map(searchResult -> (byte[]) searchResult.getField(SERIALIZED_DOCUMENT))
                        .filter(Objects::nonNull)
                        .map(redisSerializer::deserialize)
                        .collect(toList()))
                .orElseGet(Collections::emptyList);
    }

    /**
     * Simple method to handle the stopWatch and logging requirements around a given RedisClient operation
     */
    protected <N> N performTimedOperation(String name, Supplier<N> supplier) {

        StopWatch stopWatch = new StopWatch(name);
        stopWatch.start();

        N entity = supplier.get();

        stopWatch.stop();
        logger.debug("{}", stopWatch.prettyPrint());

        return entity;
    }

    public static String getIndex(Class<?> clazz) {

        return stream(clazz.getAnnotations())
                .filter(annotation -> annotation instanceof RediSearchEntity)
                .findAny()
                .map(RediSearchEntity.class::cast)
                .map(RediSearchEntity::name)
                .get();
    }

    @Override
    public SearchResults findAll(Integer offset,
                                 Integer limit,
                                 boolean includeContent) {

        offset = ofNullable(offset).orElse(0);
        limit = ofNullable(limit).orElse(defaultMaxResults.intValue());

        RediSearchOptions options = new RediSearchOptions();
        options.setLimit(Long.valueOf(limit));
        options.setOffset(Long.valueOf(offset));
        options.setNoContent(!includeContent);

        return performTimedOperation("findAll", () -> search(ALL_QUERY, options));
    }

    @Override
    public SearchResults findByFields(Map<String, String> fieldNameValues,
                                      @Nullable Long offset,
                                      @Nullable Long limit) {

        RediSearchOptions options = new RediSearchOptions();
        options.setLimit(limit);
        options.setOffset(offset);
        return findByFields(fieldNameValues, options);
    }

    @Override
    public SearchResults findByFields(Map<String, String> fieldNameValues,
                                      RediSearchOptions options) {

        fieldNameValues.forEach((name, value) -> options.addField(getField(name), value));
        return find(options);
    }

    protected String getQualifiedKey(String key) {

        return keyPrefix + key;
    }

    protected abstract SearchResults search(String queryString, RediSearchOptions searchOptions);
}