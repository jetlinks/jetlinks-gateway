package org.jetlinks.gateway.vertx.mqtt;

import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttServerOptions;
import lombok.SneakyThrows;
import org.jetlinks.core.device.DeviceMessageHandler;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.StandaloneDeviceMessageHandler;
import org.jetlinks.core.server.monitor.GatewayServerMetrics;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.supports.DefaultProtocolSupports;
import org.jetlinks.supports.server.monitor.MicrometerGatewayServerMetrics;
import org.jetlinks.supports.server.session.DefaultDeviceSessionManager;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

public class MqttServerTest {

    private DeviceRegistry registry;

    private DeviceMessageHandler deviceMessageHandler = new StandaloneDeviceMessageHandler();

    @Before
    public void init() {
        MqttServer mqttServer = new MqttServer();

        DefaultDeviceSessionManager sessionManager = new DefaultDeviceSessionManager();
        mqttServer.setMessageHandler((session, message) -> Mono.just(true));
        mqttServer.setDeviceSessionManager(sessionManager);
        DefaultProtocolSupports protocolSupports = new DefaultProtocolSupports();
        mqttServer.setProtocolSupports(protocolSupports);
        mqttServer.setRegistry(registry = new TestDeviceRegistry(protocolSupports, deviceMessageHandler));
        mqttServer.setGatewayServerMonitor(
                new GatewayServerMonitor() {
                    @Override
                    public String getCurrentServerId() {
                        return "test";
                    }

                    @Override
                    public GatewayServerMetrics metrics() {
                        return new MicrometerGatewayServerMetrics(getCurrentServerId());
                    }
                }
        );
        mqttServer.setMqttServerOptions(new MqttServerOptions());

        Vertx.vertx().deployVerticle(mqttServer);

    }

    @Test
    @SneakyThrows
    public void test(){

        Thread.sleep(1000);
    }
}