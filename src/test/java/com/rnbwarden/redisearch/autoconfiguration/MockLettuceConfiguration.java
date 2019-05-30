package com.rnbwarden.redisearch.autoconfiguration;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import io.lettuce.core.codec.RedisCodec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Configuration
public class MockLettuceConfiguration {

    @Bean
    @Primary
    public RediSearchClient rediSearchClient() {

        RediSearchClient rediSearchClient = mock(RediSearchClient.class);
        StatefulRediSearchConnection<String, String> statefulRediSearchConnection = mock(StatefulRediSearchConnection.class);

        when(rediSearchClient.connect()).thenReturn(statefulRediSearchConnection);
        when(rediSearchClient.connect(any(RedisCodec.class))).thenReturn(statefulRediSearchConnection);

        RediSearchCommands rediSearchCommands = mock(RediSearchCommands.class);
        when(statefulRediSearchConnection.sync()).thenReturn(rediSearchCommands);

        when(rediSearchCommands.info()).thenReturn("test");

        return rediSearchClient;

    }
}
