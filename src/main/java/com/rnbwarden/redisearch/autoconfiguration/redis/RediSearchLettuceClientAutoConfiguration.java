package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.CompressingJacksonSerializer;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.nio.ByteBuffer;

@Configuration("RediSearchLettuceClientAutoConfiguration")
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchConfiguration.class)
public class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private com.redislabs.lettusearch.RediSearchClient client;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        CompressingJacksonSerializer<?> compressingJacksonSerializer = super.createRedisSerializer(clazz);

        RedisCodec redisCodec = compressingRedisCodec(compressingJacksonSerializer);
        StatefulRediSearchConnection<Object, Object> statefulRediSearchConnection = client.connect(redisCodec);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new LettuceRediSearchClient(statefulRediSearchConnection, compressingJacksonSerializer, defaultMaxResults));
    }

    private RedisCodec<String, Object> compressingRedisCodec(CompressingJacksonSerializer<?> compressingJacksonSerializer) {

        return new RedisCodec<String, Object>() {

            @Override
            public String decodeKey(ByteBuffer bytes) {

                return (String) compressingJacksonSerializer.deserialize(bytes.array());
            }

            @Override
            public Object decodeValue(ByteBuffer bytes) {

                return compressingJacksonSerializer.deserialize(bytes.array());
            }

            @Override
            public ByteBuffer encodeKey(String key) {

                return ByteBuffer.wrap(compressingJacksonSerializer.serialize(key));
            }

            @Override
            public ByteBuffer encodeValue(Object value) {

                return ByteBuffer.wrap(compressingJacksonSerializer.serialize(value));
            }
        };
    }
}