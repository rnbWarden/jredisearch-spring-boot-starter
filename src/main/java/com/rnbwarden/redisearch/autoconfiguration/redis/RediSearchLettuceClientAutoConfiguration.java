package com.rnbwarden.redisearch.autoconfiguration.redis;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.springredisearch.RediSearchConfiguration;
import com.rnbwarden.redisearch.redis.client.lettuce.LettuceRediSearchClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
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

//        RedisCodec<Object, Object> redisCodec = CompressionCodec.valueCompressor(StringCodec.UTF8, CompressionCodec.CompressionType.GZIP);
        RedisCodec redisCodec = compressingRedisCodec();
        StatefulRediSearchConnection<Object, Object> statefulRediSearchConnection = client.connect(redisCodec);

        beanFactory.registerSingleton(getRedisearchBeanName(clazz), new LettuceRediSearchClient(statefulRediSearchConnection, createRedisSerializer(clazz), defaultMaxResults));
    }

    private RedisCodec<String, byte[]> compressingRedisCodec() {

        return new RedisCodec<String, byte[]>() {

            private RedisCodec<String, String> keyCodec = StringCodec.UTF8;
            private RedisCodec<byte[], byte[]> valueCodec = ByteArrayCodec.INSTANCE;

            @Override
            public String decodeKey(ByteBuffer bytes) {

                return keyCodec.decodeKey(bytes);
            }

            @Override
            public byte[] decodeValue(ByteBuffer bytes) {

                return valueCodec.decodeValue(bytes);
            }

            @Override
            public ByteBuffer encodeKey(String key) {

                return keyCodec.encodeKey(key);
            }

            @Override
            public ByteBuffer encodeValue(byte[] value) {

                return valueCodec.encodeValue(value);
            }
        };
    }
}