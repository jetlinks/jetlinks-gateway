package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MqttMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class MqttDeviceSession implements DeviceSession {

    @Getter
    private MqttEndpoint endpoint;

    @Getter
    private DeviceOperator operator;

    private long connectTime = System.currentTimeMillis();

    private volatile long lastPingTime = System.currentTimeMillis();

    private boolean checkPingTime = !Boolean.getBoolean("mqtt.check-ping.disabled");

    private int keepAliveTimeOut;

    @Getter
    private String id;

    @Getter
    private Transport transport;

    private List<Runnable> closeListener = new CopyOnWriteArrayList<>();

    public MqttDeviceSession(String id, Transport transport, MqttEndpoint endpoint, DeviceOperator operator) {
        endpoint.pingHandler(r -> {
            ping();
            if (!endpoint.isAutoKeepAlive()) {
                endpoint.pong();
            }
        });
        this.id = id;
        this.endpoint = endpoint;
        this.operator = operator;
        this.transport = transport;
        //ping 超时时间
        keepAliveTimeOut = (endpoint.keepAliveTimeSeconds() + 5) * 1000;
    }

    @Override
    public DeviceOperator getOperator() {
        return operator;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public String getDeviceId() {
        return operator.getDeviceId();
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

        }finally {
            closeListener.forEach(Runnable::run);
        }
    }

    @Override
    public Mono<Boolean> send(EncodedMessage encodedMessage) {
        return Mono.create((sink) -> {
            ping();
            if (encodedMessage instanceof MqttMessage) {
                MqttMessage message = ((MqttMessage) encodedMessage);
                Buffer buffer = Buffer.buffer(message.getPayload());
                if (log.isDebugEnabled()) {
                    log.debug("send mqtt message [{}]=>[{}]:{}", message.getTopic(), getDeviceId(), buffer.toString(StandardCharsets.UTF_8));
                }
                if (message.getMessageId() != -1) {
                    endpoint.publish(message.getTopic(),
                            buffer,
                            MqttQoS.valueOf(message.getQosLevel()),
                            message.isDup(),
                            message.isRetain(),
                            message.getMessageId(),
                            integerAsyncResult -> {
                                if (integerAsyncResult.succeeded()) {
                                    sink.success(true);
                                } else {
                                    sink.error(integerAsyncResult.cause());
                                }
                            });
                } else {
                    endpoint.publish(message.getTopic(),
                            buffer,
                            MqttQoS.valueOf(message.getQosLevel()),
                            message.isDup(),
                            message.isRetain(),
                            integerAsyncResult -> {
                                if (integerAsyncResult.succeeded()) {
                                    sink.success(true);
                                } else {
                                    sink.error(integerAsyncResult.cause());
                                }
                            });
                }

                return;
            } else {
                log.error("不支持发送消息{}到MQTT:", encodedMessage);
            }
            sink.success(false);
        });
    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        boolean connected = endpoint.isConnected();

        if (!checkPingTime) {
            return connected;
        }
        boolean isKeepAliveTimeOut = System.currentTimeMillis() - lastPingTime > keepAliveTimeOut;

        if (connected && isKeepAliveTimeOut && log.isInfoEnabled()) {
            log.info("设备[{}]已经[{}s]未发送ping", getDeviceId(), (System.currentTimeMillis() - lastPingTime) / 1000);
        }
        return connected && !isKeepAliveTimeOut;
    }


    @Override
    public void onClose(Runnable call) {
        closeListener.add(call);
    }

    @Override
    public String toString() {
        return "MQTT Session[" + getDeviceId() + "]";
    }
}
