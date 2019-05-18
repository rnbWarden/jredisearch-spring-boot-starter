package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.nio.ByteBuffer;
import java.util.Objects;

@Configuration("RediSearchLettuceClientAutoConfiguration")
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchConfiguration.class)
public class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private com.redislabs.lettusearch.RediSearchClient rediSearchClient;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        RedisSerializer<?> redisSerializer = useCompression ? super.createRedisSerializer(clazz) : createJackson2JsonRedisSerializer(clazz);
        RedisCodec redisCodec = useCompression ? compressingRedisCodec(redisSerializer) : StringCodec.UTF8;

        StatefulRediSearchConnection<String, String> statefulRediSearchConnection = rediSearchClient.connect(redisCodec);
        LettuceRediSearchClient lettuceRediSearchClient = new LettuceRediSearchClient(clazz, statefulRediSearchConnection, redisSerializer, defaultMaxResults, primaryObjectMapper);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), lettuceRediSearchClient);
    }

    private <T> RedisCodec<String, T> compressingRedisCodec(RedisSerializer<T> redisSerializer) {

        return new RedisCodec<String, T>() {

            @Override
            public String decodeKey(ByteBuffer bytes) {

                return bytes.hasArray() ? new String(bytes.array()) : null;
            }

            @Override
            public T decodeValue(ByteBuffer bytes) {

                return bytes.hasArray() ? redisSerializer.deserialize(bytes.array()) : null;
            }

            @Override
            public ByteBuffer encodeKey(String key) {

                return ByteBuffer.wrap(key.getBytes());
            }

            @Override
            public ByteBuffer encodeValue(T value) {

                byte[] serialize = redisSerializer.serialize(value);
                return ByteBuffer.wrap(Objects.requireNonNull(serialize));
            }
        };
    }
}