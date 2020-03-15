package com.rnbwarden.redisearch.config.factorybean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.client.RediSearchClient;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.data.redis.serializer.RedisSerializer;

public abstract class AbstractRediSearchClientFactoryBean<E extends RedisSearchableEntity> extends AbstractFactoryBean<RediSearchClient<E>> implements RediSearchClientFactoryBean {

    @Autowired
    @Qualifier("rediSearchObjectMapper")
    private ObjectMapper rediSearchObjectMapper;

    @Value("${redis.search.defaultResultLimit:1000000}")
    protected Long defaultMaxResults;

    protected Class<E> clazz;

    public void setClazz(Class<E> clazz) {

        this.clazz = clazz;
    }

    RedisSerializer<E> createRedisSerializer() {

        return new CompressingJacksonSerializer<>(clazz, rediSearchObjectMapper);
    }

    @Override
    public Class<?> getObjectType() {

        return clazz;
    }

    @Override
    protected com.rnbwarden.redisearch.client.RediSearchClient<E> createInstance() throws Exception {

        return createRediSearchClient();
    }

    abstract RediSearchClient<E> createRediSearchClient();
}
