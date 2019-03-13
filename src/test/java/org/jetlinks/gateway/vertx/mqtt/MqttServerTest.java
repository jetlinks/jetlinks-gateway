package org.jetlinks.gateway.vertx.mqtt;

import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttServerOptions;
import org.jetlinks.gateway.session.DefaultDeviceSessionManager;
import org.jetlinks.registry.api.*;
import org.jetlinks.registry.redis.RedissonDeviceMessageHandler;
import org.jetlinks.registry.redis.RedissonDeviceRegistry;
import org.redisson.api.RedissonClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MqttServerTest {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        RedissonClient client = RedissonHelper.newRedissonClient();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        DeviceRegistry registry = new RedissonDeviceRegistry(client,
                (request, deviceOperation) -> AuthenticationResponse.success(),
                executorService);
        DeviceInfo deviceInfo = new DeviceInfo();

        deviceInfo.setId("test");
        deviceInfo.setType((byte) 1);
        registry.registry(deviceInfo);

        MqttServer server = new MqttServer();
        server.setVertx(vertx);
        server.setDeviceSessionManager(new DefaultDeviceSessionManager());
        server.setMqttServerOptions(new MqttServerOptions());
        server.setMessageConsumer(msg -> {
            System.out.println("收到消息:" + msg.toJson());
        });

        server.setProtocolSupports(new MockProtocolSupports());
        server.setRegistry(registry);
        server.start();

    }

}