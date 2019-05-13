package com.rnbwarden.redisearch.autoconfiguration.redis;

import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.ApplicationContextAware;

public interface RediSearchClientAutoConfiguration extends BeanFactoryAware, ApplicationContextAware {

    void init();
}