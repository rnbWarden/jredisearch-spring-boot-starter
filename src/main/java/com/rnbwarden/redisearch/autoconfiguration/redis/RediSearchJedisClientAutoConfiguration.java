package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.jedis.JedisRediSearchClient;
import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

@Configuration("RediSearchJedisClientAutoConfiguration")
@ConditionalOnClass({GenericObjectPool.class, JedisConnection.class, Jedis.class, Client.class})
public class RediSearchJedisClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;
    @Autowired
    private JedisSearchConnectionFactory jedisSearchConnectionFactory;

    private JedisSentinelPool jedisSentinelPool;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new JedisRediSearchClient(createClient(clazz), createRedisSerializer(clazz), defaultMaxResults));
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