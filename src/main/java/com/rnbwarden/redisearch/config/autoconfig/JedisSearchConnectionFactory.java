package com.rnbwarden.redisearch.config.autoconfig;

import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

@Component
public class JedisSearchConnectionFactory {

    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;

    private JedisSentinelPool jedisSentinelPool;

    public Client getClient(String indexName, RedisSentinelConfiguration sentinelConfiguration) {

        return new Client(indexName, getJedisSentinelPool(sentinelConfiguration));
    }

    private JedisSentinelPool getJedisSentinelPool(RedisSentinelConfiguration sentinelConfiguration) {

        if (jedisSentinelPool == null) {
            String master = getMaster(sentinelConfiguration);
            Set<String> sentinels = getSentinels(sentinelConfiguration);
            int timeout = jedisConnectionFactory.getTimeout();
            int poolSize = getPoolSize();
            String password = jedisConnectionFactory.getPassword();
            jedisSentinelPool = new JedisSentinelPool(master, sentinels, initPoolConfig(poolSize), timeout, password);
        }
        return jedisSentinelPool;
    }

    public Client getClientForStandalone(String indexName) {

        String hostName = jedisConnectionFactory.getHostName();
        int port = jedisConnectionFactory.getPort();
        int timeout = jedisConnectionFactory.getTimeout();
        int maxPoolSize = Optional.ofNullable(jedisConnectionFactory.getPoolConfig())
                .map(GenericObjectPoolConfig::getMaxTotal)
                .orElse(100);
        return new Client(indexName, hostName, port, timeout, maxPoolSize);
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