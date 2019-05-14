package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration("RediSearchLettuceClientAutoConfiguration")
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchConfiguration.class)
public class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private com.redislabs.lettusearch.RediSearchClient client;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        RedisCodec redisCodec = CompressionCodec.valueCompressor(StringCodec.UTF8, CompressionCodec.CompressionType.GZIP);
        StatefulRediSearchConnection<byte[], byte[]> statefulRediSearchConnection = client.connect(redisCodec);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new LettuceRediSearchClient(statefulRediSearchConnection, createRedisSerializer(clazz), defaultMaxResults));
    }
}