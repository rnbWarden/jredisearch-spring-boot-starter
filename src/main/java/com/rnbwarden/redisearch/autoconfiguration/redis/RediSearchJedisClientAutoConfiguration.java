package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.rnbwarden.redisearch.redis.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.client.jedis.JedisRediSearchClient;
import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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

@Configuration
@ConditionalOnClass({GenericObjectPool.class, JedisConnection.class, Jedis.class, Client.class})
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
class RediSearchJedisClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    private final JedisConnectionFactory jedisConnectionFactory;
    private JedisSentinelPool jedisSentinelPool;

    RediSearchJedisClientAutoConfiguration(JedisConnectionFactory jedisConnectionFactory) {

        this.jedisConnectionFactory = jedisConnectionFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new JedisRediSearchClient(createClient(clazz), createRedisSerializer(clazz)));
    }

    private Client createClient(Class<?> clazz) {

        String indexName = AbstractRediSearchClient.getIndex(clazz);

        RedisSentinelConfiguration sentinelConfiguration = jedisConnectionFactory.getSentinelConfiguration();
        if (sentinelConfiguration == null) {
            return getClientForStandalone(indexName);
        }
        return new Client(indexName, getJedisSentinelPool(sentinelConfiguration));
    }

    private JedisSentinelPool getJedisSentinelPool(RedisSentinelConfiguration sentinelConfiguration) {

        if (jedisSentinelPool != null) {
            return jedisSentinelPool;
        }
        String master = getMaster(sentinelConfiguration);

        Set<String> sentinels = getSentinels(sentinelConfiguration);

        int timeout = jedisConnectionFactory.getTimeout();

        int poolSize = getPoolSize();

        String password = jedisConnectionFactory.getPassword();

        jedisSentinelPool = new JedisSentinelPool(master, sentinels, initPoolConfig(poolSize), timeout, password);
        return jedisSentinelPool;
    }

    private Client getClientForStandalone(String indexName) {

        String hostName = jedisConnectionFactory.getHostName();
        int port = jedisConnectionFactory.getPort();
        return new Client(indexName, hostName, port);
    }

    private Set<String> getSentinels(RedisSentinelConfiguration sentinelConfiguration) {

        Set<RedisNode> sentinels = sentinelConfiguration.getSentinels();
        return sentinels.stream()
                .map(sentinel -> format("%s:%s", sentinel.getHost(), sentinel.getPort()))
                .collect(toSet());
    }

    private String getMaster(RedisSentinelConfiguration sentinelConfiguration) {

        NamedNode master = sentinelConfiguration.getMaster();
        return master.getName();
    }

    private int getPoolSize() {

        GenericObjectPoolConfig poolConfig = jedisConnectionFactory.getPoolConfig();
        return poolConfig.getMaxTotal();
    }

    /**
     * Constructs JedisPoolConfig object.
     *
     * @param poolSize size of the JedisPool
     * @return {@link JedisPoolConfig} object with a few default settings
     */
    private static JedisPoolConfig initPoolConfig(int poolSize) {

        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(poolSize);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);
        conf.setFairness(true);

        return conf;
    }
}