package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchConfiguration.class)
@SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private RediSearchClient rediSearchClient;

    StatefulRediSearchConnection<String, String> statefulRediSearchConnection;

    @Override
    void init() {

        this.statefulRediSearchConnection = rediSearchClient.connect();
        super.init();
    }

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new LettuceRediSearchClient(statefulRediSearchConnection, createRedisSerializer(clazz)));
    }
}