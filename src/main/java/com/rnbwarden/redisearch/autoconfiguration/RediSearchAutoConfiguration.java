package com.rnbwarden.redisearch.autoconfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@Import({RediSearchJedisClientAutoConfiguration.class, RediSearchLettuceClientAutoConfiguration.class})
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

    @Autowired
    private RediSearchClientAutoConfiguration rediSearchClientAutoConfiguration;
}