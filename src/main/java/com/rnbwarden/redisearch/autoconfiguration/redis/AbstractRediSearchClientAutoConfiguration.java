package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.entity.RediSearchEntity;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import javax.annotation.PostConstruct;
import java.util.Set;

@EnableConfigurationProperties(RedisProperties.class)
public abstract class AbstractRediSearchClientAutoConfiguration implements BeanFactoryAware, ApplicationContextAware {

    @Value("${redis.search.base-package}")
    protected String basePackage;

    @Autowired
    protected ObjectMapper primaryObjectMapper;

    protected DefaultListableBeanFactory beanFactory;
    protected ApplicationContext applicationContext;

    @PostConstruct
    void init() {

        initializeRediSearchBeans();
    }

    private void initializeRediSearchBeans() {

        getRediSearchEntities().stream()
                .map(BeanDefinition::getBeanClassName)
                .map(className -> {
                    try {
                        return Class.forName(className, false, applicationContext.getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .forEach(this::createRediSearchBeans);
    }

    private Set<BeanDefinition> getRediSearchEntities() {

        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RediSearchEntity.class));

        return scanner.findCandidateComponents(basePackage);
    }

    abstract void createRediSearchBeans(Class<?> clazz);

    String getRedisearchBeanName(Class<?> clazz) {

        String simpleName = clazz.getSimpleName();
        simpleName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        return simpleName + "RediSearchClient";
    }

    CompressingJacksonSerializer<?> createRedisSerializer(Class<?> clazz) {

        return new CompressingJacksonSerializer<>(clazz, primaryObjectMapper);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {

        this.beanFactory = (DefaultListableBeanFactory) beanFactory;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

}
