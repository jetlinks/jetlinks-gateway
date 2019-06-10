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
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.MqttAuthenticationRequest;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.EmptyDeviceMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.gateway.monitor.GatewayServerMonitor;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.session.DeviceSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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

    @Getter
    @Setter
    private GatewayServerMonitor gatewayServerMonitor;

    @Getter
    @Setter
    private String publicServerAddress;

    @Override
    public void start() {
        Objects.requireNonNull(deviceSessionManager);
        Objects.requireNonNull(protocolSupports);
        Objects.requireNonNull(mqttServerOptions);
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(registry);
        if (publicServerAddress == null) {
            publicServerAddress = (mqttServerOptions.isSsl() ? "ssl://" : "tcp://") + "127.0.0.1:" + mqttServerOptions.getPort();
        }
        io.vertx.mqtt.MqttServer mqttServer = io.vertx.mqtt.MqttServer.create(vertx, mqttServerOptions);
        mqttServer.endpointHandler(this::doConnect)
                .exceptionHandler(err -> logger.error(err.getMessage(), err))
                .listen(result -> {
                    if (result.succeeded()) {
                        int port = result.result().actualPort();
                        if (gatewayServerMonitor != null) {
                            gatewayServerMonitor.registerTransport(Transport.MQTT, publicServerAddress);
                        }
                        logger.info("MQTT 服务启动成功,端口:{},最大连接数限制:{},公共服务地址:{} ",
                                port,
                                deviceSessionManager.getMaximumConnection(Transport.MQTT),
                                publicServerAddress);
                    } else {
                        logger.error("MQTT 服务启动失败!", result.cause());
                    }
                });
    }

    protected void doConnect(MqttEndpoint endpoint) {
        try {
            if (endpoint.auth() == null) {
                endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
                return;
            }
            if (deviceSessionManager.isOutOfMaximumConnectionLimit(Transport.MQTT)) {
                //当前连接超过了最大连接数
                logger.info("拒绝客户端连接[{}],已超过最大连接数限制:[{}]!", endpoint.clientIdentifier(), deviceSessionManager.getMaximumConnection(Transport.MQTT));
                endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
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
            operation.authenticate(new MqttAuthenticationRequest(
                    clientId, userName, passWord
            )).whenComplete((response, err) -> {
                if (err != null) {
                    logger.warn("设备认证[{}]失败", clientId, err);
                    endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                } else {
                    if (response.isSuccess()) {
                        MqttDeviceSession session = new MqttDeviceSession(endpoint, () -> registry.getDevice(clientId));
                        accept(endpoint, session);
                    } else if (401 == response.getCode()) {
                        logger.debug("设备[{}]认证未通过:{}", clientId, response);
                        endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                    } else {
                        logger.warn("设备[{}]认证失败:{}", clientId, response);
                        endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                    }
                }
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
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
            }
        }
    }

    protected void accept(MqttEndpoint endpoint, MqttDeviceSession session) {
        String clientId = endpoint.clientIdentifier();
        try {
            //注册
            deviceSessionManager.register(session);
            logger.debug("MQTT客户端[{}]建立连接", clientId);
            endpoint
                    //订阅请求
                    .subscribeHandler(subscribe -> {
                        List<MqttQoS> grantedQosLevels = subscribe.topicSubscriptions()
                                .stream()
                                .map(MqttTopicSubscription::qualityOfService)
                                .collect(Collectors.toList());
                        if (logger.isDebugEnabled()) {
                            logger.debug("SUBSCRIBE: [{}] 订阅topics {} messageId={}", clientId,
                                    subscribe.topicSubscriptions()
                                            .stream()
                                            .collect(Collectors.toMap(MqttTopicSubscription::topicName, MqttTopicSubscription::qualityOfService))
                                    , subscribe.messageId());
                        }
                        endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
                    })
                    //QoS 1 PUBACK
                    .publishAcknowledgeHandler(messageId -> logger.debug("PUBACK: [{}]发送消息[{}]已确认.", clientId, messageId))
                    //QoS 2  PUBREC
                    .publishReceivedHandler(messageId -> {
                        logger.debug("PUBREC: [{}]的消息[{}]已收到.", clientId, messageId);
                        endpoint.publishRelease(messageId);
                    })
                    //QoS 2  PUBREL
                    .publishReleaseHandler(messageId -> {
                        logger.debug("PUBREL: 释放[{}]消息:{}",clientId, messageId);
                        endpoint.publishComplete(messageId);
                    })
                    //QoS 2  PUBCOMP
                    .publishCompletionHandler(messageId -> logger.debug("PUBCOMP: [{}]消息[{}]处理完成.", clientId, messageId))

                    //取消订阅 UNSUBSCRIBE
                    .unsubscribeHandler(unsubscribe -> {
                        logger.debug("UNSUBSCRIBE: [{}]取消订阅:{}", clientId, unsubscribe.topics());
                        endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
                    })
                    //断开连接 DISCONNECT
                    .disconnectHandler(v -> {
                        logger.debug("MQTT客户端[{}]断开连接", clientId);
                        doCloseEndpoint(endpoint);
                    })
                    //接收客户端推送的消息
                    .publishHandler(message -> {
                        //设备推送了消息
                        String topicName = message.topicName();
                        Buffer buffer = message.payload();
                        if (logger.isDebugEnabled()) {
                            logger.debug("收到设备[{}]消息[{}]=>{}", clientId, topicName, buffer.toString());
                        }
                        try {
                            EncodedMessage encodedMessage = new VertxMqttMessage(clientId, message);
                            //转换消息未可读对象
                            DeviceMessage deviceMessage = session.getProtocolSupport()
                                    .getMessageCodec()
                                    .decode(Transport.MQTT, new FromDeviceMessageContext() {
                                        @Override
                                        public DeviceOperation getDeviceOperation() {
                                            return session.getOperation();
                                        }

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

                                    });
                            //处理消息回复
                            if (deviceMessage instanceof DeviceMessageReply) {
                                getDeviceSessionManager()
                                        .handleDeviceMessageReply(session, ((DeviceMessageReply) deviceMessage));
                            }
                            //推送事件
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
                    .exceptionHandler(e -> {
                        logger.debug("MQTT客户端[{}]连接错误", clientId, e);
                        doCloseEndpoint(endpoint);
                    })
                    .closeHandler(v -> doCloseEndpoint(endpoint))
                    .accept(false);
        } catch (Exception e) {
            logger.error("建立MQTT连接失败,client:{}", clientId, e);
            doCloseEndpoint(endpoint);
        }
    }
}
