package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import static org.mockito.Mockito.mock;

@Configuration
public class MockConfiguration {

    @Bean
    public ObjectMapper objectMapper() {

        return mock(ObjectMapper.class);
    }

    @Bean
    public JedisConnectionFactory jedisConnectionFactory() {

        return mock(JedisConnectionFactory.class);
    }

}
