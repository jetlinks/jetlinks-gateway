package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.messages.MqttPublishMessage;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.AuthenticationResponse;
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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

    protected String getClientId(MqttEndpoint endpoint) {
        return endpoint.clientIdentifier();
    }

    protected CompletionStage<AuthenticationResponse> doAuth(MqttEndpoint endpoint) {
        if (endpoint.auth() == null) {
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
            return CompletableFuture.completedFuture(AuthenticationResponse.error(401, "未提供认证信息"));
        }
        String clientId = getClientId(endpoint);
        String userName = endpoint.auth().getUsername();
        String passWord = endpoint.auth().getPassword();
        DeviceOperation operation = registry.getDevice(clientId);
        if (operation.getState() == DeviceState.unknown) {
            return CompletableFuture.completedFuture(AuthenticationResponse.error(401, "未注册到注册中心"));
        } else {
            return operation.authenticate(new MqttAuthenticationRequest(clientId, userName, passWord));
        }
    }

    private void doConnect(MqttEndpoint endpoint) {
        try {
            if (deviceSessionManager.isOutOfMaximumConnectionLimit(Transport.MQTT)) {
                //当前连接超过了最大连接数
                logger.info("拒绝客户端连接[{}],已超过最大连接数限制:[{}]!", endpoint.clientIdentifier(), deviceSessionManager.getMaximumConnection(Transport.MQTT));
                endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                return;
            }
            String clientId = getClientId(endpoint);
            //进行认证
            doAuth(endpoint)
                    .whenComplete((response, err) -> {
                        if (err != null) {
                            logger.warn("设备认证[{}]失败:{}", clientId, err.getMessage(), err);
                            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                        } else {
                            if (response.isSuccess()) {
                                String deviceId = Optional.ofNullable(response.getDeviceId()).orElse(clientId);
                                String sessionId = deviceId;
                                //返回了新的deviceId,可能是多个设备公用一个设备注册认证.或者自动注册的设备.
                                if (!deviceId.equals(clientId)) {
                                    if (registry.getDevice(deviceId).getState() == DeviceState.unknown) {
                                        logger.info("设备[{}]认证通过,但是返回的新设备ID[{}]未注册", clientId, deviceId);
                                        endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                                        return;
                                    }
                                    //构造sessionId
                                    sessionId = clientId.concat(":").concat(deviceId);
                                }

                                MqttDeviceSession session = new MqttDeviceSession(sessionId, endpoint, registry::getDevice);

                                session.setDeviceId(deviceId);

                                accept(endpoint, session);
                            } else if (401 == response.getCode()) {
                                logger.info("设备[{}]认证未通过:{}", clientId, response.getMessage());
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

    protected void doCloseEndpoint(MqttEndpoint client, String deviceId) {
        logger.debug("关闭客户端[{}]MQTT连接", deviceId);
        DeviceSession old = deviceSessionManager.unregister(deviceId);
        if (old == null) {
            if (client.isConnected()) {
                client.close();
            }
        }
    }

    protected void accept(MqttEndpoint endpoint, MqttDeviceSession session) {
        String deviceId = session.getDeviceId();
        try {
            //注册
            deviceSessionManager.register(session);
            logger.debug("MQTT客户端[{}]建立连接", deviceId);
            endpoint
                    //订阅请求
                    .subscribeHandler(subscribe -> {
                        List<MqttQoS> grantedQosLevels = subscribe.topicSubscriptions()
                                .stream()
                                .map(MqttTopicSubscription::qualityOfService)
                                .collect(Collectors.toList());
                        if (logger.isDebugEnabled()) {
                            logger.debug("SUBSCRIBE: [{}] 订阅topics {} messageId={}", deviceId,
                                    subscribe.topicSubscriptions()
                                            .stream()
                                            .collect(Collectors.toMap(MqttTopicSubscription::topicName, MqttTopicSubscription::qualityOfService))
                                    , subscribe.messageId());
                        }
                        endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
                    })
                    //QoS 1 PUBACK
                    .publishAcknowledgeHandler(messageId -> logger.debug("PUBACK: [{}]发送消息[{}]已确认.", deviceId, messageId))
                    //QoS 2  PUBREC
                    .publishReceivedHandler(messageId -> {
                        logger.debug("PUBREC: [{}]的消息[{}]已收到.", deviceId, messageId);
                        endpoint.publishRelease(messageId);
                    })
                    //QoS 2  PUBREL
                    .publishReleaseHandler(messageId -> {
                        logger.debug("PUBREL: 释放[{}]消息:{}", deviceId, messageId);
                        endpoint.publishComplete(messageId);
                    })
                    //QoS 2  PUBCOMP
                    .publishCompletionHandler(messageId -> logger.debug("PUBCOMP: [{}]消息[{}]处理完成.", deviceId, messageId))

                    //取消订阅 UNSUBSCRIBE
                    .unsubscribeHandler(unsubscribe -> {
                        logger.debug("UNSUBSCRIBE: [{}]取消订阅:{}", deviceId, unsubscribe.topics());
                        endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
                    })
                    //断开连接 DISCONNECT
                    .disconnectHandler(v -> {
                        logger.debug("MQTT客户端[{}]断开连接", deviceId);
                        doCloseEndpoint(endpoint, deviceId);
                    })
                    //接收客户端推送的消息
                    .publishHandler(message -> handleMqttMessage(session, endpoint, message))
                    .exceptionHandler(e -> {
                        logger.debug("MQTT客户端[{}]连接错误", deviceId, e);
                        doCloseEndpoint(endpoint, deviceId);
                    })
                    .closeHandler(v -> doCloseEndpoint(endpoint, deviceId))
                    .accept(false);
        } catch (Exception e) {
            logger.error("建立MQTT连接失败,client:{}", deviceId, e);
            doCloseEndpoint(endpoint, deviceId);
        }
    }

    protected DeviceMessage decodeMessage(DeviceSession session, MqttEndpoint endpoint, VertxMqttMessage message) {
        String deviceId = message.getDeviceId();
        return session.getProtocolSupport()
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
                        doCloseEndpoint(endpoint, deviceId);
                    }

                    @Override
                    public EncodedMessage getMessage() {
                        return message;
                    }

                });
    }

    protected void handleMqttMessage(DeviceSession session, MqttEndpoint endpoint, MqttPublishMessage message) {
        session.ping();
        String deviceId = session.getDeviceId();
        //设备推送了消息
        String topicName = message.topicName();
        Buffer buffer = message.payload();
        if (logger.isDebugEnabled()) {
            logger.debug("收到设备[{}]消息[{}:{}]=>{}", deviceId, topicName, message.messageId(), buffer.toString());
        }
        try {
            VertxMqttMessage encodedMessage = new VertxMqttMessage(deviceId, message);
            //转换消息为可读到消息对象
            DeviceMessage deviceMessage = decodeMessage(session, endpoint, encodedMessage);
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
            logger.error("处理设备[{}]消息[{}]:\n{}\n失败", deviceId, topicName, buffer.toString(), e);
        } finally {
            if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                endpoint.publishAcknowledge(message.messageId());
            } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                endpoint.publishReceived(message.messageId());
            }
        }
    }
}
