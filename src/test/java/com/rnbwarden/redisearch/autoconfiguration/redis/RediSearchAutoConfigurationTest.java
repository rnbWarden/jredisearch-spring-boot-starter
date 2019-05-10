package com.rnbwarden.redisearch.autoconfiguration.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {
        RediSearchAutoConfiguration.class,
        MockConfiguration.class
})
public class RediSearchAutoConfigurationTest {

    @Autowired
    private BeanFactory beanFactory;

    @Test
    public void testAutoConfig() {

        assertThat(beanFactory).isNotNull();
        assertThat(beanFactory.getBean("stubEntityRedisSearchClient")).isNotNull();
//        assertThat(beanFactory.getBean("stubEntityClient")).isNotNull();
//        assertThat(beanFactory.getBean("stubEntityRedisSerializer")).isNotNull();
    }
}