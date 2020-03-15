package com.rnbwarden.redisearch.config.autoconfig;

import com.redislabs.springredisearch.RediSearchAutoConfiguration;
import com.rnbwarden.redisearch.config.factorybean.RediSearchClientFactoryBean;
import com.rnbwarden.redisearch.config.factorybean.RediSearchLettuceClientFactoryBean;
import io.lettuce.core.RedisClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration("RediSearchLettuceClientAutoConfiguration")
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchAutoConfiguration.class)
public class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Override
    Class<? extends RediSearchClientFactoryBean> getFactoryBeanClass() {

        return RediSearchLettuceClientFactoryBean.class;
    }
}