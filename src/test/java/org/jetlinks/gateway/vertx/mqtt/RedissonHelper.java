package org.jetlinks.gateway.vertx.mqtt;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class RedissonHelper {

    public static RedissonClient newRedissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(System.getProperty("redis.host", "redis://127.0.0.1:6379"))
                .setDatabase(0);


        return Redisson.create(config);
    }
}
