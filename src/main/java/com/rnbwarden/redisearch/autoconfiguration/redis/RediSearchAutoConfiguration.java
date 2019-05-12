package com.rnbwarden.redisearch.autoconfiguration.redis;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@Import({RediSearchJedisClientAutoConfiguration.class,
        RediSearchLettuceClientAutoConfiguration.class})
public class RediSearchAutoConfiguration {

}