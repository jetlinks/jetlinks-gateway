package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MqttMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.gateway.session.DeviceSession;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class MqttDeviceSession implements DeviceSession {

    private MqttEndpoint endpoint;

    @Getter
    private  Function<String,DeviceOperation>  operationSupplier;

    private long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    private int keepAliveTimeOut;

    @Setter
    private String deviceId;

    @Getter
    private String id;

    public MqttDeviceSession(String id, MqttEndpoint endpoint, Function<String,DeviceOperation> operation) {
        endpoint.pingHandler(r -> ping());
        this.id=id;
        this.endpoint = endpoint;
        this.operationSupplier = operation;
        //ping 超时时间
        keepAliveTimeOut = (endpoint.keepAliveTimeSeconds() + 2) * 1000;
    }

    @Override
    public ProtocolSupport getProtocolSupport() {
        return getOperation().getProtocol();
    }

    @Override
    public DeviceOperation getOperation() {
        return operationSupplier.apply(deviceId);
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public String getDeviceId() {
        return deviceId == null ? endpoint.clientIdentifier() : deviceId;
    }

    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public void close() {
        try {
            if (endpoint.isConnected()) {
                endpoint.close();
            }
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
            endpoint.publish(message.getTopic(), buffer, MqttQoS.valueOf(message.getQosLevel()), message.isDup(), message.isRetain());
        } else {
            log.error("不支持发送消息{}到MQTT:", encodedMessage);
        }
    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
        if (!endpoint.isAutoKeepAlive()) {
            endpoint.pong();
        }
    }

    @Override
    public boolean isAlive() {
        boolean isKeepAliveTimeOut = System.currentTimeMillis() - lastPingTime > keepAliveTimeOut;

        if (isKeepAliveTimeOut && log.isInfoEnabled()) {
            log.info("设备[{}],ping超时[{}s]!", getDeviceId(), endpoint.keepAliveTimeSeconds());
        }
        return endpoint.isConnected() && !isKeepAliveTimeOut;
    }

    @Override
    public String toString() {
        return "MQTT Client[" + getDeviceId() + "]";
    }
}
