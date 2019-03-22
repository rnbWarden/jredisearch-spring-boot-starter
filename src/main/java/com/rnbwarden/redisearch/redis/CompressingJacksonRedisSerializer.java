package com.rnbwarden.redisearch.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.lang.System.currentTimeMillis;

public class CompressingJacksonRedisSerializer<T> extends Jackson2JsonRedisSerializer<T> {

    private static final int WORK_BUFFER_SIZE = 8192;
    private final Logger logger = LoggerFactory.getLogger(CompressingJacksonRedisSerializer.class);
    private final Class<T> clazz;

    public CompressingJacksonRedisSerializer(Class<T> type, ObjectMapper objectMapper) {

        super(type);
        setObjectMapper(objectMapper);
        this.clazz = type;
    }

    /**
    public CompressingJacksonRedisSerializer(JavaType javaType, ObjectMapper objectMapper) {

        super(javaType);
        setObjectMapper(objectMapper);
        this.clazz = (Class<T>) javaType.getRawClass();
    }*/

    @Override
    public T deserialize(byte[] bytes) throws SerializationException {

        long startTime = currentTimeMillis();

        ByteArrayOutputStream baos = new ByteArrayOutputStream(WORK_BUFFER_SIZE);

        final byte[] buffer = new byte[WORK_BUFFER_SIZE];

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             GZIPInputStream gzis = new GZIPInputStream(bais, WORK_BUFFER_SIZE)) {
            int n;
            while (-1 != (n = gzis.read(buffer))) {
                baos.write(buffer, 0, n);
            }

        } catch (IOException e) {
            throw new SerializationException("Could not decompress.", e);
        }

        logger.debug("Decompressed. Orig: {}, Decompressed: {}, Time: {}", bytes.length, baos.size(), currentTimeMillis() - startTime);
        return super.deserialize(baos.toByteArray());
    }

    @Override
    public byte[] serialize(Object t) throws SerializationException {

        long startTime = currentTimeMillis();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] serializedObject = super.serialize(t);

        try (GZIPOutputStream gzos = new GZIPOutputStream(baos, WORK_BUFFER_SIZE)) {
            gzos.write(serializedObject);
        } catch (IOException e) {
            throw new SerializationException("Could not compress serialize.", e);
        }

        logger.debug("Compressed. Orig: {}, Compressed: {}, Time: {}, Pct: {}", serializedObject.length, baos.size(),
                currentTimeMillis() - startTime,
                100 - (((double) (baos.size())) / ((double) serializedObject.length)) * 100
        );

        return baos.toByteArray();
    }

    public Class<T> getClazz() {

        return clazz;
    }
}
