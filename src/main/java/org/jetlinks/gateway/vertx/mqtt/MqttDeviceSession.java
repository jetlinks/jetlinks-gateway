package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MqttMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.device.DeviceOperation;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class MqttDeviceSession implements DeviceSession {

    private MqttEndpoint endpoint;

    @Getter
    private Supplier<DeviceOperation> operationSupplier;

    private long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    public MqttDeviceSession(MqttEndpoint endpoint, Supplier<DeviceOperation> operation) {
        endpoint.pingHandler(r -> ping());
        this.endpoint = endpoint;
        this.operationSupplier = operation;
    }

    @Override
    public ProtocolSupport getProtocolSupport() {
        return getOperation().getProtocol();
    }

    @Override
    public DeviceOperation getOperation() {
        return operationSupplier.get();
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
                log.debug("发送消息到MQTT客户端[{}]=>[{}]:{}", message.getTopic(), getDeviceId(), buffer.toString(StandardCharsets.UTF_8));
            }
            endpoint.publish(message.getTopic(), buffer, MqttQoS.valueOf(message.getQosLevel()), false, false);
        } else {
            log.error("不支持发送消息{}到MQTT:", encodedMessage);
        }
    }

    @Override
    public void ping() {
//        log.info("mqtt client[{}] ping", getClientId());
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
