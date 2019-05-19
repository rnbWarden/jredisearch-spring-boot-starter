package com.rnbwarden.redisearch.autoconfiguration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@Import({RediSearchJedisClientAutoConfiguration.class, RediSearchLettuceClientAutoConfiguration.class})
public class RediSearchAutoConfiguration {

    @Autowired
    private RediSearchClientAutoConfiguration rediSearchClientAutoConfiguration;
}