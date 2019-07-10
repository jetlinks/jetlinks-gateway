package org.jetlinks.gateway.monitor;

import io.lettuce.core.RedisClient;

public class RedisClientHelper {


    public static RedisClient createRedisClient() {

        return RedisClient.create(System.getProperty("redis.host", "redis://127.0.0.1:6379"));
    }


}
