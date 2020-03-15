package com.rnbwarden.redisearch.config.autoconfig;

import com.rnbwarden.redisearch.client.RediSearchClient;
import com.rnbwarden.redisearch.config.factorybean.RediSearchClientFactoryBean;
import com.rnbwarden.redisearch.entity.RediSearchEntity;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.util.Set;

public abstract class AbstractRediSearchClientAutoConfiguration implements ApplicationContextAware, BeanDefinitionRegistryPostProcessor {

    private ApplicationContext applicationContext;

    private Set<BeanDefinition> getRediSearchEntityBeanDefinitions() {

        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RediSearchEntity.class));
        String basePackage = applicationContext.getEnvironment().getProperty("redis.search.base-package");
        return scanner.findCandidateComponents(basePackage);
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

        getRediSearchEntityBeanDefinitions().forEach(beanDefinition -> {
            String className = beanDefinition.getBeanClassName();
            try {
                Class<?> clazz = Class.forName(className, false, applicationContext.getClassLoader());
                if (RedisSearchableEntity.class.isAssignableFrom(clazz)) {
                    String factoryBeanName = getRediSearchClientFactoryBeanName(clazz);
                    RootBeanDefinition rootBeanDefinition = createFactoryBeanDefinition(clazz, factoryBeanName);
                    registry.registerBeanDefinition(factoryBeanName, rootBeanDefinition);
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String getRediSearchClientFactoryBeanName(Class<?> clazz) {

        String simpleName = clazz.getSimpleName();
        simpleName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        return simpleName + "RediSearchClient";
    }

    private RootBeanDefinition createFactoryBeanDefinition(Class<?> clazz, String factoryBeanName) {

        RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
        ResolvableType targetType = ResolvableType.forClassWithGenerics(RediSearchClient.class, clazz);
        rootBeanDefinition.setTargetType(targetType);
        rootBeanDefinition.setBeanClass(getFactoryBeanClass());
        rootBeanDefinition.getPropertyValues().add("clazz", clazz);
        rootBeanDefinition.setDescription(factoryBeanName);
        return rootBeanDefinition;
    }

    abstract Class<? extends RediSearchClientFactoryBean> getFactoryBeanClass();

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }
}