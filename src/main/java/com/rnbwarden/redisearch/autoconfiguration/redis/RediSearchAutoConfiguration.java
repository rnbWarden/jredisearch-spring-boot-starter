package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.rnbwarden.redisearch.redis.AbstractRediSearchClient;
import com.rnbwarden.redisearch.redis.CompressingJacksonRedisSerializer;
import com.rnbwarden.redisearch.redis.RediSearchEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.redisearch.client.Client;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import javax.annotation.PostConstruct;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for RediSearch.
 */
@Configuration("RediSearchAutoConfiguration")
@ConditionalOnClass(io.redisearch.client.Client.class)
@EnableConfigurationProperties(RedisProperties.class)
public class RediSearchAutoConfiguration implements BeanFactoryAware, ApplicationContextAware {

    @Value("${redis.search.base-package}")
    public String basePackage;

    @Autowired
    private ObjectMapper primaryObjectMapper;

    @Autowired
    private JedisConnectionFactory jedisConnectionFactory;

    private DefaultListableBeanFactory beanFactory;
    private ApplicationContext applicationContext;
    private JedisSentinelPool jedisSentinelPool;


    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {

        this.beanFactory = (DefaultListableBeanFactory) beanFactory;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

    @PostConstruct
    private void initializeRediSearchBeans() {

        getRediSearchEntities().stream()
                .map(BeanDefinition::getBeanClassName)
                .forEach(this::createRediSearchBeans);
    }

    private Set<BeanDefinition> getRediSearchEntities() {

        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RediSearchEntity.class));

        return scanner.findCandidateComponents(basePackage);
    }

    private void createRediSearchBeans(String className) {

        try {
            Class<?> clazz = Class.forName(className, false, applicationContext.getClassLoader());
            Client client = createClient(clazz);

            String simpleName = clazz.getSimpleName();
            simpleName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);

            beanFactory.registerSingleton(simpleName + "Client", client);
            beanFactory.registerSingleton(simpleName + "RedisSerializer", new CompressingJacksonRedisSerializer<>(clazz, primaryObjectMapper));

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Client createClient(Class<?> clazz) {

        String indexName = AbstractRediSearchClient.getIndex(clazz);
        return createClient(indexName);
    }

    private Client createClient(String indexName) {

        RedisSentinelConfiguration sentinelConfiguration = jedisConnectionFactory.getSentinelConfiguration();
        if (sentinelConfiguration == null) {
            return getClientForStandalone(indexName);
        }
        return new Client(indexName, getJedisSentinelPool(sentinelConfiguration));
    }

    private JedisSentinelPool getJedisSentinelPool(RedisSentinelConfiguration sentinelConfiguration) {

        if (jedisSentinelPool != null) {
            return jedisSentinelPool;
        }
        String master = getMaster(sentinelConfiguration);

        Set<String> sentinels = getSentinels(sentinelConfiguration);

        int timeout = jedisConnectionFactory.getTimeout();

        int poolSize = getPoolSize();

        String password = jedisConnectionFactory.getPassword();

        jedisSentinelPool = new JedisSentinelPool(master, sentinels, initPoolConfig(poolSize), timeout, password);
        return jedisSentinelPool;
    }

    private Client getClientForStandalone(String indexName) {

        String hostName = jedisConnectionFactory.getHostName();
        int port = jedisConnectionFactory.getPort();
        return new Client(indexName, hostName, port);
    }

    private Set<String> getSentinels(RedisSentinelConfiguration sentinelConfiguration) {

        Set<RedisNode> sentinels = sentinelConfiguration.getSentinels();
        return sentinels.stream()
                .map(sentinel -> format("%s:%s", sentinel.getHost(), sentinel.getPort()))
                .collect(toSet());
    }

    private String getMaster(RedisSentinelConfiguration sentinelConfiguration) {

        NamedNode master = sentinelConfiguration.getMaster();
        return master.getName();
    }

    private int getPoolSize() {

        GenericObjectPoolConfig poolConfig = jedisConnectionFactory.getPoolConfig();
        return poolConfig.getMaxTotal();
    }

    /**
     * Constructs JedisPoolConfig object.
     *
     * @param poolSize size of the JedisPool
     * @return {@link JedisPoolConfig} object with a few default settings
     */
    private static JedisPoolConfig initPoolConfig(int poolSize) {

        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(poolSize);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);
        conf.setFairness(true);

        return conf;
    }
}