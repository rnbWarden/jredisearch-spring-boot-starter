package com.rnbwarden.redisearch.autoconfiguration;

import com.redislabs.springredisearch.RediSearchAutoConfiguration;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.nio.ByteBuffer;

@Configuration("RediSearchLettuceClientAutoConfiguration")
@ConditionalOnClass({RedisClient.class, com.redislabs.lettusearch.RediSearchClient.class})
@Import(RediSearchAutoConfiguration.class)
public class RediSearchLettuceClientAutoConfiguration extends AbstractRediSearchClientAutoConfiguration {

    @Autowired
    private com.redislabs.lettusearch.RediSearchClient rediSearchClient;

    @Override
    @SuppressWarnings("unchecked")
    void createRediSearchBeans(Class<?> clazz) {

        RedisSerializer<?> redisSerializer = createRedisSerializer(clazz);
        RedisCodec redisCodec = new LettuceRedisCodec();

        LettuceRediSearchClient lettuceRediSearchClient = new LettuceRediSearchClient(clazz, rediSearchClient, redisCodec, redisSerializer, defaultMaxResults);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), lettuceRediSearchClient);
    }

    public static class LettuceRedisCodec implements RedisCodec<String, Object> {

        private ByteArrayCodec byteArrayCodec = new ByteArrayCodec();
        private StringCodec stringCodec = new Utf8StringCodec();

        @Override
        public ByteBuffer encodeKey(String key) {

            return stringCodec.encodeKey(key);
        }

        @Override
        public String decodeKey(ByteBuffer bytes) {

            return stringCodec.decodeKey(bytes);
        }

        @Override
        public ByteBuffer encodeValue(Object value) {

            if (value instanceof byte[]) {
                return ByteBuffer.wrap((byte[]) value);
            }
            return stringCodec.encodeValue((String) value);
        }

        @Override
        public Object decodeValue(ByteBuffer bytes) {

            return byteArrayCodec.decodeValue(bytes);
        }
    }
}