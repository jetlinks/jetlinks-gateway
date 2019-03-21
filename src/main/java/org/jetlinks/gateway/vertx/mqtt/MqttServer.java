package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.session.DeviceSessionManager;
import org.jetlinks.protocol.ProtocolSupport;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.message.EmptyDeviceMessage;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.codec.FromDeviceMessageContext;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.protocol.metadata.DeviceMetadata;
import org.jetlinks.registry.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MqttServer extends AbstractVerticle {

    @Getter
    @Setter
    private Logger logger = LoggerFactory.getLogger(MqttServer.class);

    @Getter
    @Setter
    private DeviceRegistry registry;

    @Getter
    @Setter
    private MqttServerOptions mqttServerOptions;

    @Getter
    @Setter
    private DeviceSessionManager deviceSessionManager;

    @Getter
    @Setter
    private BiConsumer<DeviceSession, DeviceMessage> messageConsumer;

    @Getter
    @Setter
    private ProtocolSupports protocolSupports;

    @Override
    public void start() {
        Objects.requireNonNull(deviceSessionManager);
        Objects.requireNonNull(protocolSupports);
        Objects.requireNonNull(mqttServerOptions);
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(registry);

        io.vertx.mqtt.MqttServer mqttServer = io.vertx.mqtt.MqttServer.create(vertx, mqttServerOptions);
        mqttServer.endpointHandler(this::doConnect)
                .exceptionHandler(err -> logger.error(err.getMessage(), err))
                .listen(result -> {
                    if (result.succeeded()) {
                        int port = result.result().actualPort();
                        logger.debug("MQTT server started on port {}", port);
                    } else {
                        logger.warn("MQTT server start failed", result.cause());
                    }
                });
    }

    protected void doConnect(MqttEndpoint endpoint) {
        if (endpoint.auth() == null) {
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
            return;
        }
        String clientId = endpoint.clientIdentifier();
        String userName = endpoint.auth().getUsername();
        String passWord = endpoint.auth().getPassword();
        DeviceOperation operation = registry.getDevice(clientId);
        if (operation.getState() == DeviceState.unknown) {
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
            return;
        }
        //进行认证
        AuthenticationResponse response = operation.authenticate(new UsernamePasswordAuthenticationRequest(
                clientId, userName, passWord
        ));
        //授权通过
        if (response.isSuccess()) {
            String protocol = operation.getDeviceInfo().getProtocol();
            ProtocolSupport protocolSupport = getProtocol(protocol);
            MqttDeviceSession session = new MqttDeviceSession(endpoint, operation, protocolSupport);
            accept(endpoint, session);
        } else if (401 == response.getCode()) {
            logger.debug("设备[{}]认证未通过", clientId, response);
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
        } else {
            logger.warn("设备[{}]认证失败", clientId, response);
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
        }
    }

    protected void doCloseEndpoint(MqttEndpoint client) {
        String clientId = client.clientIdentifier();
        logger.debug("关闭客户端[{}]MQTT连接", clientId);
        DeviceSession old = deviceSessionManager.unregister(clientId);
        if (old == null) {
            if (client.isConnected()) {
                client.close();
            } else {
                client.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            }
        }
    }

    protected ProtocolSupport getProtocol(String protocol) {
        return protocolSupports.getProtocol(protocol);
    }

    protected void accept(MqttEndpoint endpoint, MqttDeviceSession session) {
        String clientId = endpoint.clientIdentifier();
        try {
            //注册
            deviceSessionManager.register(session);
            logger.info("MQTT客户端[{}]建立链接", clientId);
            endpoint
                    .closeHandler(v -> doCloseEndpoint(endpoint))
                    .subscribeHandler(subscribe -> {
                        List<MqttQoS> grantedQosLevels = new ArrayList<>();
                        for (MqttTopicSubscription s : subscribe.topicSubscriptions()) {
                            grantedQosLevels.add(s.qualityOfService());
                        }
                        endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
                        endpoint.publishAcknowledgeHandler(messageId -> logger.info("[{}] Received ack for message = {}", clientId, messageId))
                                .publishReceivedHandler(endpoint::publishRelease)
                                .publishCompletionHandler(messageId -> logger.info("[{}] Received ack for message = {}", clientId, messageId));
                    })
                    .unsubscribeHandler(unsubscribe -> endpoint.unsubscribeAcknowledge(unsubscribe.messageId()))
                    .disconnectHandler(v -> {
                        logger.info("MQTT客户端[{}]断开链接", clientId);
                        doCloseEndpoint(endpoint);
                    })
                    .exceptionHandler(e -> {
                        logger.error("MQTT客户端[{}]链接错误", clientId, e);
                        doCloseEndpoint(endpoint);
                    })
                    .publishHandler(message -> {
                        //设备推送了消息
                        String topicName = message.topicName();
                        Buffer buffer = message.payload();
                        if (logger.isDebugEnabled()) {
                            logger.debug("收到设备[{}]消息{}=>{}", topicName, clientId, buffer.toString());
                        }
                        try {
                            //消息协议
                            EncodedMessage encodedMessage = EncodedMessage.mqtt(clientId, topicName, buffer.getByteBuf());
                            //转换消息
                            DeviceMessage deviceMessage = session.getProtocolSupport()
                                    .getMessageCodec()
                                    .decode(Transport.MQTT, new FromDeviceMessageContext() {
                                        @Override
                                        public void sendToDevice(EncodedMessage message) {
                                            session.send(message);
                                        }

                                        @Override
                                        public void disconnect() {
                                            doCloseEndpoint(endpoint);
                                        }

                                        @Override
                                        public EncodedMessage getMessage() {
                                            return encodedMessage;
                                        }

                                        @Override
                                        public DeviceMetadata getDeviceMetadata() {
                                            return session.getOperation().getMetadata();
                                        }
                                    });
                            if (!(deviceMessage instanceof EmptyDeviceMessage)) {
                                messageConsumer.accept(session, deviceMessage);
                            }
                        } catch (Throwable e) {
                            logger.error("处理设备[{}]消息[{}]:\n{}\n失败", clientId, topicName, buffer.toString(), e);
                        } finally {
                            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                                endpoint.publishAcknowledge(message.messageId());
                            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                                endpoint.publishReceived(message.messageId());
                            }
                        }
                    })
                    .publishReleaseHandler(messageId -> {
                        logger.debug("complete message :{}", messageId);
                        endpoint.publishComplete(messageId);
                    })
                    .accept(false);
        } catch (Exception e) {
            logger.error("建立MQTT连接失败,client:{}", clientId, e);
            doCloseEndpoint(endpoint);
        }
    }
}
