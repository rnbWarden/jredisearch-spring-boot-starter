package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
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
//    private StatefulRediSearchConnection<String, String> statefulRediSearchConnection;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        ByteArrayCodec byteArrayCodec = ByteArrayCodec.INSTANCE;
        StatefulRediSearchConnection<byte[], byte[]> statefulRediSearchConnection = client.connect(byteArrayCodec);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new LettuceRediSearchClient(statefulRediSearchConnection, createRedisSerializer(clazz), defaultMaxResults));
    }
}