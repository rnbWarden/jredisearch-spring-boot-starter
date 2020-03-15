package com.rnbwarden.redisearch.config.autoconfig;

import io.redisearch.client.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Configuration
public class MockJedisConfiguration {

    @Bean
    public JedisSearchConnectionFactory jedisSearchConnectionFactory() {

        JedisSearchConnectionFactory jedisSearchConnectionFactory = mock(JedisSearchConnectionFactory.class);
        Client client = mock(Client.class);
        when(jedisSearchConnectionFactory.getClientForStandalone(anyString())).thenReturn(client);
        return jedisSearchConnectionFactory;
    }

    @Bean
    public JedisConnectionFactory jedisConnectionFactory() {

        JedisConnectionFactory mock = mock(JedisConnectionFactory.class);
        when(mock.getHostName()).thenReturn("localhost");
        when(mock.getPort()).thenReturn(6379);
        return mock;
    }
}
