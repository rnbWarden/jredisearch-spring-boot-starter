package com.rnbwarden.redisearch.config.factorybean;

import com.rnbwarden.redisearch.client.AbstractRediSearchClient;
import com.rnbwarden.redisearch.client.RediSearchClient;
import com.rnbwarden.redisearch.client.jedis.JedisRediSearchClient;
import com.rnbwarden.redisearch.config.autoconfig.JedisSearchConnectionFactory;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.redisearch.client.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class RediSearchJedisClientFactoryBean<E extends RedisSearchableEntity> extends AbstractRediSearchClientFactoryBean<E> {

    private final JedisConnectionFactory jedisConnectionFactory;
    private final JedisSearchConnectionFactory jedisSearchConnectionFactory;

    @Autowired
    public RediSearchJedisClientFactoryBean(JedisConnectionFactory jedisConnectionFactory,
                                            JedisSearchConnectionFactory jedisSearchConnectionFactory) {

        this.jedisConnectionFactory = jedisConnectionFactory;
        this.jedisSearchConnectionFactory = jedisSearchConnectionFactory;
    }

    @Override
    RediSearchClient<E> createRediSearchClient() {

        Client client = createClient();
        RedisSerializer<E> redisSerializer = createRedisSerializer();
        return new JedisRediSearchClient<>(clazz, client, redisSerializer);
    }

    private Client createClient() {

        String indexName = AbstractRediSearchClient.getIndex(clazz);

        RedisSentinelConfiguration sentinelConfiguration = jedisConnectionFactory.getSentinelConfiguration();
        if (sentinelConfiguration != null) {
            return jedisSearchConnectionFactory.getClient(indexName, sentinelConfiguration);
        }
        return jedisSearchConnectionFactory.getClientForStandalone(indexName);
    }

    @Override
    protected RediSearchClient<E> createInstance() throws Exception {

        return createRediSearchClient();
    }
}
