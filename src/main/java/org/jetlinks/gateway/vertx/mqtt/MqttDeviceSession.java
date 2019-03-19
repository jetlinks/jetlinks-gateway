package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.MqttMessage;
import org.jetlinks.protocol.message.codec.Transport;

import java.nio.charset.StandardCharsets;

/**
 * @author zhouhao
 * @since 1.1.0
 */
@Slf4j
public class MqttDeviceSession implements DeviceSession {

    private MqttEndpoint endpoint;

    private long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    public MqttDeviceSession(MqttEndpoint endpoint) {
        endpoint.pingHandler(r -> ping());
        this.endpoint = endpoint;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public String getId() {
        return getDeviceId();
    }

    @Override
    public String getDeviceId() {
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
    public Transport getTransport() {
        return Transport.MQTT;
    }

    @Override
    public void send(EncodedMessage encodedMessage) {
        if (encodedMessage instanceof MqttMessage) {
            MqttMessage message = ((MqttMessage) encodedMessage);
            ping();
            Buffer buffer = Buffer.buffer(message.getByteBuf());
            if (log.isDebugEnabled()) {
                log.debug("发送消息到客户端[{}]=>[{}]:{}", message.getTopic(), getDeviceId(), buffer.toString(StandardCharsets.UTF_8));
            }
            endpoint.publish(message.getTopic(), buffer, MqttQoS.AT_MOST_ONCE, false, false);
        } else {
            log.error("不支持发送消息{}到MQTT:", encodedMessage);
        }
    }

    @Override
    public void ping() {
//        log.info("mqtt client[{}] ping", getDeviceId());
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        return endpoint.isConnected();
    }

    @Override
    public String toString() {
        return "MQTT Client[" + getDeviceId() + "]";
    }
}
