package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceClient;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.MqttMessage;

import java.nio.charset.StandardCharsets;

/**
 * @author zhouhao
 * @since 1.1.0
 */
@Slf4j
public class MqttClient implements DeviceClient {

    private MqttEndpoint endpoint;

    private long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    public MqttClient(MqttEndpoint endpoint) {
        endpoint.pingHandler(r -> ping());
        this.endpoint = endpoint;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public String getId() {
        return getClientId();
    }

    @Override
    public String getClientId() {
        return endpoint.clientIdentifier();
    }

    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public void close() {
        try {
            endpoint.close();
        } catch (Exception ignore) {

        }
    }

    @Override
    public void send(EncodedMessage encodedMessage) {
        if (encodedMessage instanceof MqttMessage) {
            MqttMessage message = ((MqttMessage) encodedMessage);
            ping();
            Buffer buffer = Buffer.buffer(message.getByteBuf());
            if (log.isDebugEnabled()) {
                log.debug("发送消息到客户端[{}]=>[{}]:{}", message.getTopic(), getClientId(), buffer.toString(StandardCharsets.UTF_8));
            }
            endpoint.publish(message.getTopic(), buffer, MqttQoS.AT_MOST_ONCE, false, false);
        } else {
            log.error("不支持发送消息{}到mqtt:", encodedMessage);
        }
    }

    @Override
    public void ping() {
        log.debug("mqtt client[{}] ping", getClientId());
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        return endpoint.isConnected();
    }

    @Override
    public String toString() {
        return "MQTT Client[" + getClientId() + "]";
    }
}
