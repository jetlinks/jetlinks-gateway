package org.jetlinks.gateway.vertx.mqtt;

import io.netty.buffer.Unpooled;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttServerOptions;
import org.jetlinks.gateway.session.DefaultDeviceSessionManager;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.device.AuthenticationResponse;
import org.jetlinks.protocol.device.DeviceInfo;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.registry.api.*;
import org.jetlinks.registry.redis.RedissonDeviceMonitor;
import org.jetlinks.registry.redis.RedissonDeviceRegistry;
import org.redisson.api.RedissonClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MqttServerTest {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        RedissonClient client = RedissonHelper.newRedissonClient();

        ProtocolSupports protocolSupports = new MockProtocolSupports();

        DeviceRegistry registry = new RedissonDeviceRegistry(client,
                protocolSupports);
        DeviceInfo deviceInfo = new DeviceInfo();

        deviceInfo.setId("test");
        deviceInfo.setType((byte) 1);
        registry.registry(deviceInfo);

        DefaultDeviceSessionManager deviceSessionManager = new DefaultDeviceSessionManager();
        deviceSessionManager.setDeviceMonitor(new RedissonDeviceMonitor(client));
        deviceSessionManager.setExecutorService(Executors.newScheduledThreadPool(2));
        deviceSessionManager.setDeviceRegistry(registry);
        deviceSessionManager.setServerId("test");
        deviceSessionManager.setProtocolSupports(protocolSupports);
        MqttServerOptions mqttServerOptions = new MqttServerOptions();

        mqttServerOptions.setPort(1884);

        MqttServer server = new MqttServer();
        server.setDeviceSessionManager(deviceSessionManager);
        server.setMqttServerOptions(mqttServerOptions);
        server.setMessageConsumer((deviceClient, msg) -> {
            System.out.println("收到消息:" + msg.toJson());

            deviceClient.send(EncodedMessage.mqtt(deviceInfo.getId(), "test", Unpooled.copiedBuffer("msg".getBytes())));
        });

        server.setProtocolSupports(protocolSupports);
        server.setRegistry(registry);
        vertx.deployVerticle(server);
    }

}