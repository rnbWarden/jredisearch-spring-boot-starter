package com.rnbwarden.redisearch.autoconfiguration.redis;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@EnableConfigurationProperties(RedisProperties.class)
@Import({RediSearchJedisClientAutoConfiguration.class,
        RediSearchLettuceClientAutoConfiguration.class})
public class RediSearchAutoConfiguration {

}