package com.rnbwarden.redisearch.config.autoconfig;

import com.rnbwarden.redisearch.config.factorybean.RediSearchClientFactoryBean;
import com.rnbwarden.redisearch.config.factorybean.RediSearchJedisClientFactoryBean;
import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import redis.clients.jedis.Jedis;

@Configuration("RediSearchJedisClientAutoConfiguration")
@ConditionalOnClass({GenericObjectPool.class, JedisConnection.class, Jedis.class, Client.class})
@ComponentScan(basePackages = "com.rnbwarden.redisearch.autoconfig")
public class RediSearchJedisClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Override
    Class<? extends RediSearchClientFactoryBean> getFactoryBeanClass() {

        return RediSearchJedisClientFactoryBean.class;
    }
}