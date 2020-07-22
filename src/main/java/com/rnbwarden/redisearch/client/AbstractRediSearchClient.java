package com.rnbwarden.redisearch.client;

import com.rnbwarden.redisearch.client.context.PagingSearchContext;
import com.rnbwarden.redisearch.client.context.SearchContext;
import com.rnbwarden.redisearch.entity.*;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public abstract class AbstractRediSearchClient<E extends RedisSearchableEntity, T extends SearchableField<E>> implements RediSearchClient<E> {

    protected static final String SERIALIZED_DOCUMENT = "sdoc";
    protected static final String ALL_QUERY = "*";

    private final Logger logger = LoggerFactory.getLogger(AbstractRediSearchClient.class);
    protected final String index;
    protected final String keyPrefix;

    protected final RedisSerializer<E> redisSerializer;
    private final Map<String, T> fields = new LinkedHashMap<>();
    private final Class<E> clazz;

    protected AbstractRediSearchClient(Class<E> clazz,
                                       RedisSerializer<E> redisSerializer) {

        this.clazz = clazz;
        this.redisSerializer = redisSerializer;
        this.index = getIndex(clazz);
        this.keyPrefix = format("%s:", index);
        initSearchableFields(clazz);
    }

    public Class<E> getType() {

        return clazz;
    }

    private void initSearchableFields(Class<E> clazz) {

        getSearchableFieldsFromFields(clazz).forEach(field -> fields.put(field.getName(), field));
        getSearchFieldsFromMethods(clazz).forEach(field -> fields.put(field.getName(), field));
    }

    protected abstract void checkAndCreateIndex();

    protected abstract T createSearchableField(RediSearchFieldType type,
                                               String name,
                                               boolean sortable,
                                               Function<E, String> serializationFunction);

    private List<T> getSearchableFieldsFromFields(Class<E> clazz) {

        return FieldUtils.getFieldsListWithAnnotation(clazz, RediSearchField.class).stream()
                .map(field -> stream(field.getAnnotations())
                        .filter(annotation -> annotation instanceof RediSearchField)
                        .map(RediSearchField.class::cast)
                        .map(annotation -> createSearchableField(field, annotation))
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(toList());
    }

    private T createSearchableField(Field field, RediSearchField annotation) {

        RediSearchFieldType type = annotation.type();
        if (type == RediSearchFieldType.NO_INDEX) {
            return createSearchableField(type, annotation.name(), annotation.sortable(), e -> getFieldValue(field, e, this::getSerializedObjectValue));
        }
        return createSearchableField(type, annotation.name(), annotation.sortable(), e -> getFieldValue(field, e, this::getQueryableSerializedObjectValue));
    }

    private String getFieldValue(Field f, E obj, Function<Object, String> valueSerializationFunction) {

        try {
            boolean accessible = f.isAccessible();
            f.setAccessible(true);
            Object object = f.get(obj);
            f.setAccessible(accessible);
            return valueSerializationFunction.apply(object);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(format("Unable to get RediSearch annotated entity value for entity: %s of class: %s", f.getName(), obj.getClass()), e);
        }
    }

    private List<T> getSearchFieldsFromMethods(Class<E> clazz) {

        return MethodUtils.getMethodsListWithAnnotation(clazz, RediSearchField.class).stream()
                .map(method -> stream(method.getAnnotations())
                        .filter(annotation -> annotation instanceof RediSearchField)
                        .map(RediSearchField.class::cast)
                        .map(annotation -> createSearchableField(method, annotation))
                        .collect(toList()))
                .flatMap(List::stream)
                .collect(toList());
    }

    private T createSearchableField(Method method, RediSearchField annotation) {

        RediSearchFieldType type = annotation.type();
        if (type == RediSearchFieldType.NO_INDEX) {
            return createSearchableField(type, annotation.name(), annotation.sortable(), e -> getFieldValue(method, e, this::getSerializedObjectValue));
        }
        return createSearchableField(type, annotation.name(), annotation.sortable(), e -> getFieldValue(method, e, this::getQueryableSerializedObjectValue));
    }

    private String getFieldValue(Method method, E obj, Function<Object, String> valueSerializationFunction) {

        try {
            Object object = method.invoke(obj, (Object[]) null);
            return valueSerializationFunction.apply(object);
        } catch (Exception ex) {
            throw new RuntimeException(String.format("cannot invoke method:%s on %s", method.getName(), obj.getClass()), ex);
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
                .map(String.class::cast)
                .collect(joining(","));
    }

    @SuppressWarnings("unchecked")
    private String getQueryableSerializedObjectValue(Object o) {

        if (o == null) {
            return null;
        }
        if (!Collection.class.isAssignableFrom(o.getClass())) {
            return QueryField.escapeSpecialCharacters(o.toString());
        }
        return (String) ((Collection) o).stream()
                .filter(Objects::nonNull)
                .map(Object::toString)
                .map(s -> QueryField.escapeSpecialCharacters((String) s))
                .collect(joining(","));
    }

    protected List<T> getFields() {

        return fields.values().stream().collect(Collectors.toUnmodifiableList());
    }

    @Override
    public SearchableField<E> getField(String name) {

        return Optional.ofNullable(fields.get(name))
                .orElseThrow(() -> new IllegalArgumentException("Invalid field name: " + name));
    }

    protected Map<String, Object> serialize(E entity) {

        Map<String, Object> serializedFields = new HashMap<>();
        fields.forEach((fieldName, field) -> {
            String serializedValue = field.serialize(entity);
            if (serializedValue != null) {
                serializedFields.put(fieldName, serializedValue);
            }
        });
        serializedFields.put(SERIALIZED_DOCUMENT, redisSerializer.serialize(entity));
        return serializedFields;
    }

    @Override
    public void recreateIndex() {

        dropIndex();
        checkAndCreateIndex();
    }

    @Override
    public Long getKeyCount() {

        SearchContext<E> searchContext = SearchContext.<E>builder().offset(0).limit(0).noContent(true).build();
        SearchResults<E> searchResults = search(ALL_QUERY, searchContext);
        return searchResults.getTotalResults();
    }

    @Override
    public Long getKeyCount(PagingSearchContext<E> pagingSearchContext) {

        AtomicLong count = new AtomicLong();
        search(pagingSearchContext).resultStream().forEach(r -> count.incrementAndGet());
        return count.get();
    }

    @Override
    public List<E> deserialize(SearchResults<E> searchResults) {

        return ofNullable(searchResults)
                .map(SearchResults::getResults)
                .map(results -> results.stream()
                        .map(searchResult -> (byte[]) searchResult.getField(SERIALIZED_DOCUMENT))
                        .filter(Objects::nonNull)
                        .map(redisSerializer::deserialize)
                        .collect(toList()))
                .orElseGet(Collections::emptyList);
    }

    public E deserialize(Map<String, Object> fields) {

        return redisSerializer.deserialize((byte[]) fields.get(SERIALIZED_DOCUMENT));
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
    public SearchContext<E> getSearchContextWithFields(Map<String, String> fieldNameValues) {

        SearchContext<E> searchContext = new SearchContext<>();
        fieldNameValues.forEach((name, value) -> searchContext.addField(getField(name), value));
        return searchContext;
    }

    @Override
    public SearchContext<E> getSearchContextWithFields(String fieldName, Collection<String> fieldValues) {

        PagingSearchContext<E> pagingSearchContext = new PagingSearchContext<>();
        pagingSearchContext.addField(getField(fieldName), fieldValues);
        return pagingSearchContext;
    }

    @Override
    public PagingSearchContext<E> getPagingSearchContextWithFields(Map<String, String> fieldNameValues) {

        PagingSearchContext<E> pagingSearchContext = new PagingSearchContext<>();
        fieldNameValues.forEach((name, value) -> pagingSearchContext.addField(getField(name), value));
        return pagingSearchContext;
    }

    @Override
    public PageableSearchResults<E> findAll(Integer limit) {

        PagingSearchContext<E> pagingSearchContext = new PagingSearchContext<>();
        ofNullable(limit).ifPresent(pagingSearchContext::setLimit);
        return findAll(pagingSearchContext);
    }

    @Override
    public PageableSearchResults<E> findAll(PagingSearchContext<E> pagingSearchContext) {

        return pagingSearch(ALL_QUERY, pagingSearchContext);
    }

    protected abstract SearchResults<E> search(String queryString, SearchContext<E> searchContext);

    protected PageableSearchResults<E> pagingSearch(String queryString, PagingSearchContext<E> pagingSearchContext) {

        assert (queryString != null);
        assert (pagingSearchContext != null);
        return pagingSearchContext.isUseClientSidePaging() ?
                clientSidePagingSearch(queryString, pagingSearchContext) :
                aggregateSearch(queryString, pagingSearchContext);
    }

    protected abstract PageableSearchResults<E> clientSidePagingSearch(String queryString,
                                                                       PagingSearchContext<E> pagingSearchContext);

    protected abstract PageableSearchResults<E> aggregateSearch(String queryString,
                                                                PagingSearchContext<E> searchContext);

    protected String getQualifiedKey(String key) {

        return keyPrefix + key;
    }

    protected String buildQueryString(SearchContext<E> searchContext) {

        List<QueryField<E>> queryFields = searchContext.getQueryFields();
        StringBuilder sb = new StringBuilder();
        queryFields.stream()
                .map(queryField -> queryField.isNegated() ?
                        format("-@%s:%s", queryField.getName(), queryField.getQuerySyntax()) :
                        format("@%s:%s", queryField.getName(), queryField.getQuerySyntax()))
                .forEach(sb::append);
        return sb.toString();
    }

}