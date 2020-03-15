package com.rnbwarden.redisearch.config.factorybean;

import com.redislabs.lettusearch.RediSearchClient;
import com.rnbwarden.redisearch.client.lettuce.LettuceRediSearchClient;
import com.rnbwarden.redisearch.entity.RedisSearchableEntity;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

@Component
public class RediSearchLettuceClientFactoryBean<E extends RedisSearchableEntity> extends AbstractRediSearchClientFactoryBean<E> {

    private final com.redislabs.lettusearch.RediSearchClient rediSearchClient;

    @Autowired
    public RediSearchLettuceClientFactoryBean(RediSearchClient rediSearchClient) {

        this.rediSearchClient = rediSearchClient;
    }

    @Override
    com.rnbwarden.redisearch.client.RediSearchClient<E> createRediSearchClient() {

        RedisSerializer<E> redisSerializer = createRedisSerializer();
        RedisCodec<String, Object> redisCodec = new LettuceRedisCodec();
        return new LettuceRediSearchClient<>(clazz, rediSearchClient, redisCodec, redisSerializer, defaultMaxResults);
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
