package com.rnbwarden.redisearch.autoconfiguration;

import com.rnbwarden.redisearch.client.jedis.JedisRediSearchClient;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

public class RediSearchAutoConfigurationTest {

    @Test
    public void testAutoConfigLettuce() {

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RediSearchAutoConfiguration.class, MockLettuceConfiguration.class))
                .withClassLoader(new FilteredClassLoader(Jedis.class))
                .withPropertyValues("redis.search.base-package=com.rnbwarden.redisearch")
                .run((context) -> {
                    assertThat(context).doesNotHaveBean(JedisRediSearchClient.class);
                    assertThat(context).hasBean("stubEntityRediSearchClient");
                });
    }

    @Test
    public void testAutoConfigJedis() {

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RediSearchAutoConfiguration.class, MockJedisConfiguration.class))
                .withClassLoader(new FilteredClassLoader(io.lettuce.core.RedisClient.class))
                .withPropertyValues("redis.search.base-package=com.rnbwarden.redisearch")
                .run((context) -> {
                    assertThat(context).doesNotHaveBean(LettuceRediSearchClient.class);
                    assertThat(context).hasBean("stubEntityRediSearchClient");
                });
    }
}