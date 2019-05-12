package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Configuration
public class MockLettuceConfiguration {

    @Bean
    public ObjectMapper objectMapper() {

        return mock(ObjectMapper.class);
    }

    @Bean
    public com.redislabs.lettusearch.RediSearchClient rediSearchClient() {

        RediSearchClient rediSearchClient = mock(RediSearchClient.class);
        StatefulRediSearchConnection<String, String> statefulRediSearchConnection = mock(StatefulRediSearchConnection.class);

        when(rediSearchClient.connect()).thenReturn(statefulRediSearchConnection);

        RediSearchCommands rediSearchCommands = mock(RediSearchCommands.class);
        when(statefulRediSearchConnection.sync()).thenReturn(rediSearchCommands);

        when(rediSearchCommands.info()).thenReturn("test");

        return rediSearchClient;

    }
}
