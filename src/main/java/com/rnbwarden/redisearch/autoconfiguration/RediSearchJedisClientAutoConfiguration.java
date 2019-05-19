package com.rnbwarden.redisearch.autoconfiguration;

import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.jedis.JedisRediSearchClient;
import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.Jedis;

import static java.lang.String.format;

@Configuration("RediSearchJedisClientAutoConfiguration")
@ConditionalOnClass({GenericObjectPool.class, JedisConnection.class, Jedis.class, Client.class})
@ComponentScan(basePackages = "com.rnbwarden.redisearch.autoconfiguration.redis")
public class RediSearchJedisClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;
    @Autowired
    private JedisSearchConnectionFactory jedisSearchConnectionFactory;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        Client client = createClient(clazz);
        RedisSerializer<?> redisSerializer = createRedisSerializer(clazz);
        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new JedisRediSearchClient(clazz, client, redisSerializer, defaultMaxResults));
    }

    private Client createClient(Class<?> clazz) {

        String indexName = AbstractRediSearchClient.getIndex(clazz);

        RedisSentinelConfiguration sentinelConfiguration = jedisConnectionFactory.getSentinelConfiguration();
        if (sentinelConfiguration != null) {
            return jedisSearchConnectionFactory.getClient(indexName, sentinelConfiguration);
        }
        return jedisSearchConnectionFactory.getClientForStandalone(indexName);
    }


}