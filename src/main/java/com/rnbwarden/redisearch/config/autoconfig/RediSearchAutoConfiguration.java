package com.rnbwarden.redisearch.config.autoconfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@Import({RediSearchJedisClientAutoConfiguration.class, RediSearchLettuceClientAutoConfiguration.class})
@EnableConfigurationProperties(RedisProperties.class)
public class RediSearchAutoConfiguration {

    @ConditionalOnMissingBean(name = "objectMapper")
    @Bean(name = "objectMapper")
    public ObjectMapper objectMapper() {

        return new ObjectMapper();
    }

    @ConditionalOnMissingBean(name = "rediSearchObjectMapper")
    @Bean(name = "rediSearchObjectMapper")
    public ObjectMapper rediSearchObjectMapper(ObjectMapper objectMapper) {

        return objectMapper;
    }
}