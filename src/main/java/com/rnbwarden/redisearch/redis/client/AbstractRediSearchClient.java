package com.rnbwarden.redisearch.redis.client;

import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.entity.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.StopWatch;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public abstract class AbstractRediSearchClient<E extends RedisSearchableEntity, S extends RediSearchOptions, T extends SearchableField<E>> implements RediSearchClient<E, S> {

    protected static final String SERIALIZED_DOCUMENT = "sdoc";
    protected static final String ALL_QUERY = "*";

    private final Logger logger = LoggerFactory.getLogger(AbstractRediSearchClient.class);

    @Value("${redis.search.defaultResultLimit:0x7fffffff}")
    protected Long defaultMaxValue;

    protected final RedisSerializer<E> redisSerializer;
    protected final List<T> fields = new ArrayList<>();
    protected final Class<E> clazz;

    protected AbstractRediSearchClient(CompressingJacksonSerializer<E> redisSerializer) {

        this.redisSerializer = redisSerializer;
        this.clazz = redisSerializer.getClazz();
        initSearchableFields();
    }

    protected void initSearchableFields() {

        fields.addAll(getSearchableFieldsFromFields());
        fields.addAll(getSearchFieldsFromMethods());
    }

    protected abstract Map<RediSearchFieldType, BiFunction<String, Function<E, Object>, T>> getFieldStrategy();

    protected abstract void checkAndCreateIndex();

    private List<T> getSearchableFieldsFromFields() {

        return FieldUtils.getFieldsListWithAnnotation(clazz, RediSearchField.class).stream()
                .map(field -> stream(field.getAnnotations())
                        .filter(annotation -> annotation instanceof RediSearchField)
                        .map(RediSearchField.class::cast)
                        .map(annotation -> getFieldStrategy().get(annotation.type()).apply(annotation.name(), e -> getFieldValue(field, e)))
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
                                return method.invoke(e, (Object[]) null);
                            } catch (Exception ex) {
                                throw new RuntimeException(String.format("cannot invoke method:%s on %s", method.getName(), e.getClass()), ex);
                            }
                        }))
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(toList());
    }

    protected static Object getFieldValue(Field f, Object obj) {

        try {
            boolean accessible = f.isAccessible();

            f.setAccessible(true);
            Object o = f.get(obj);
            f.setAccessible(accessible);

            return o;
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(format("Unable to get RediSearch annotated entity value for entity: %s of class: %s", f.getName(), obj.getClass()), e);
        }
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
        getFields().forEach(field -> fields.put(field.getName(), field.serialize(entity)));
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

    public abstract RediSearchOptions getRediSearchOptions();

    protected List<E> deserialize(SearchResults<String, Object> searchResults) {

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
    protected <T> T performTimedOperation(String name, Supplier<T> supplier) {

        StopWatch stopWatch = new StopWatch(name);
        stopWatch.start();

        T entity = supplier.get();

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

}