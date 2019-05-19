package com.rnbwarden.redisearch.entity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Designates entities eligible for Auto Configuration.
 *
 * Classes marked as RediSearchEntities will have a JRediSearch Client and RedisSerializer automatically created & wired
 * into RediSearchClient concrete classes.
 *
 * Note - it is <b>not</b> required for beans to specify this annotation, only ones designated for auto-configuration.
 */
@Retention(RUNTIME)
@Target({ElementType.TYPE})
public @interface RediSearchEntity {

    String name();
}