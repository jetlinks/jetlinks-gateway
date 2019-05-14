package org.jetlinks.gateway.vertx.mqtt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttServerOptions;
import lombok.SneakyThrows;
import org.jetlinks.gateway.monitor.RedissonGatewayServerMonitor;
import org.jetlinks.gateway.session.DefaultDeviceSessionManager;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.defaults.JetLinksProtocolSupport;
import org.jetlinks.protocol.device.AuthenticationRequest;
import org.jetlinks.protocol.device.AuthenticationResponse;
import org.jetlinks.protocol.device.DeviceInfo;
import org.jetlinks.protocol.device.DeviceOperation;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.DeviceMessageReply;
import org.jetlinks.protocol.message.property.ReadPropertyMessageReply;
import org.jetlinks.registry.api.*;
import org.jetlinks.registry.redis.RedissonDeviceMessageHandler;
import org.jetlinks.registry.redis.RedissonDeviceRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RedissonClient;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MqttServerTest {

    Vertx vertx = Vertx.vertx();
    private DeviceMessageHandler deviceMessageHandler;


    public DeviceOperation startServer(int port, Consumer<Boolean> startResultConsumer, BiConsumer<DeviceSession, DeviceMessage> messageConsumer) {
        RedissonClient client = RedissonHelper.newRedissonClient();
        ProtocolSupports protocolSupports = protocol ->
                new JetLinksProtocolSupport() {
                    @Nonnull
                    @Override
                    public AuthenticationResponse authenticate(@Nonnull AuthenticationRequest request, @Nonnull DeviceOperation deviceOperation) {
                        return AuthenticationResponse.success();
                    }
                };

        RedissonDeviceRegistry registry = new RedissonDeviceRegistry(client, protocolSupports);
        DeviceInfo deviceInfo = new DeviceInfo();

        deviceInfo.setId("test");
        deviceInfo.setType((byte) 1);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(6);
        RedissonDeviceMessageHandler handler = new RedissonDeviceMessageHandler(client, executorService);
        DefaultDeviceSessionManager deviceSessionManager = new DefaultDeviceSessionManager();
        RedissonGatewayServerMonitor monitor=    new RedissonGatewayServerMonitor("test",client,executorService);
        monitor.startup();
        deviceSessionManager.setGatewayServerMonitor(monitor);
        deviceSessionManager.setDeviceMessageHandler(deviceMessageHandler = handler);
        deviceSessionManager.setExecutorService(executorService);
        deviceSessionManager.setDeviceRegistry(registry);
        deviceSessionManager.setServerId("test");
        deviceSessionManager.setProtocolSupports(protocolSupports);
        deviceSessionManager.init();
        MqttServerOptions mqttServerOptions = new MqttServerOptions();

        mqttServerOptions.setPort(port);

        MqttServer server = new MqttServer();
        server.setDeviceSessionManager(deviceSessionManager);
        server.setMqttServerOptions(mqttServerOptions);
        server.setMessageConsumer((deviceClient, msg) -> {
            if (msg instanceof DeviceMessageReply) {
                deviceMessageHandler.reply(((DeviceMessageReply) msg));
            }
            messageConsumer.accept(deviceClient, msg);
        });

        server.setProtocolSupports(protocolSupports);
        server.setRegistry(registry);
        vertx.deployVerticle(server, result -> {
            startResultConsumer.accept(result.succeeded());
        });
        return registry.registry(deviceInfo);
    }

    @Test
    @SneakyThrows
    public void testMqtt() {
        CountDownLatch startCountdown = new CountDownLatch(1);

        int port = 11884;

        DeviceOperation operation = startServer(port,
                success -> startCountdown.countDown(),
                (deviceSession, deviceMessage) -> {
                });

        Assert.assertTrue(startCountdown.await(10, TimeUnit.SECONDS));

        CountDownLatch connectCountDown = new CountDownLatch(1);

        MqttClient client = MqttClient.create(vertx, new MqttClientOptions()
                .setClientId("test")
                .setUsername("test")
                .setPassword("test"));

        client.publishHandler(message -> {
            String data = message.payload().toString();
            System.out.println("来自服务端的消息:" + data);
            JSONObject jsonObject = JSON.parseObject(data);
            if (message.topicName().equals("/read-property")) {
                jsonObject.put("success", true);
                jsonObject.put("properties", Collections.singletonMap("name", "123"));
                client.publish("/read-property-reply", Buffer.buffer(jsonObject.toJSONString().getBytes()),
                        MqttQoS.AT_MOST_ONCE, false, false);
            }
        }).connect(port, "127.0.0.1", result -> {
            if (!result.succeeded()) {
                result.cause().printStackTrace();
            }
            connectCountDown.countDown();
        });
        Assert.assertTrue(connectCountDown.await(5, TimeUnit.SECONDS));
        ReadPropertyMessageReply reply = operation.messageSender()
                .readProperty("test")
                .send()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        System.out.println(reply.toJson().toJSONString());
        Assert.assertTrue(reply.isSuccess());


    }

}