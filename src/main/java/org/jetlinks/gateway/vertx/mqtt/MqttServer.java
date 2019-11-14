package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttWill;
import io.vertx.mqtt.messages.MqttPublishMessage;
import lombok.Getter;
import lombok.Setter;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.AuthenticationResponse;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.device.MqttAuthenticationRequest;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.MqttMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.GatewayServer;
import org.jetlinks.core.server.monitor.GatewayServerMonitor;
import org.jetlinks.core.server.mqtt.AckType;
import org.jetlinks.core.server.session.DeviceSession;
import org.jetlinks.core.server.session.DeviceSessionManager;
import org.jetlinks.supports.server.ClientMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MqttServer extends AbstractVerticle implements GatewayServer {

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
    private ProtocolSupports protocolSupports;

    @Getter
    @Setter
    private ClientMessageHandler messageHandler;

    @Getter
    @Setter
    private VertxMqttGatewayServerContext serverContext;

    @Getter
    @Setter
    private GatewayServerMonitor gatewayServerMonitor;

    @Getter
    @Setter
    private int maxBufferSize = 1024;

    @Getter
    @Setter
    private Duration acceptConnectionTimeout = Duration.ofSeconds(10);

    private AtomicInteger accepting = new AtomicInteger();

    @Override
    public Transport getTransport() {
        return mqttServerOptions.isSsl() ? DefaultTransport.MQTT_SSL : DefaultTransport.MQTT;
    }

    @Override
    public void start() {
        Objects.requireNonNull(deviceSessionManager);
        Objects.requireNonNull(mqttServerOptions);
        Objects.requireNonNull(vertx);
        Objects.requireNonNull(registry);
        Objects.requireNonNull(protocolSupports);
        if (serverContext == null) {
            serverContext = new VertxMqttGatewayServerContext();
            serverContext.setTransport(getTransport());
        }
        Flux
                .<MqttEndpoint>create(sink -> {
                    io.vertx.mqtt.MqttServer mqttServer = io.vertx.mqtt.MqttServer.create(vertx, mqttServerOptions);
                    mqttServer.endpointHandler(sink::next)
                            .exceptionHandler(err -> logger.error(err.getMessage(), err))
                            .listen(result -> {
                                if (result.succeeded()) {
                                    int port = result.result().actualPort();
                                    logger.info("MQTT started on port :{},maximum session:{}",
                                            port,
                                            deviceSessionManager.getMaximumSession(getTransport()));
                                } else {
                                    logger.error("MQTT start failed!", result.cause());
                                }
                            });
                })
                .doOnNext(e -> {
                    int waiting = accepting.getAndIncrement();
                    if (waiting > 0) {
                        logger.debug("waiting accept mqtt connections {}", waiting);
                    }
                    gatewayServerMonitor.metrics().newConnection(getTransport().getId());
                })
                .onBackpressureBuffer(
                        acceptConnectionTimeout,
                        maxBufferSize,
                        e -> {
                            logger.warn("reject client[{}], can not handle more client connection,current waiting clients : {}",
                                    e.clientIdentifier(), accepting.decrementAndGet());
                            gatewayServerMonitor.metrics().rejectedConnection(getTransport().getId());
                            e.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                        }
                )
                .flatMap(this::doConnect)
                .doOnNext(s -> {
                    accepting.decrementAndGet();
                    gatewayServerMonitor.metrics().acceptedConnection(getTransport().getId());
                })
                .subscribe(session -> serverContext.doAccept(session));


    }


    protected String getClientId(MqttEndpoint endpoint) {
        return endpoint.clientIdentifier();
    }

    protected Mono<AuthenticationResponse> doAuth(MqttEndpoint endpoint) {
        if (endpoint.auth() == null) {
            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
            return Mono.just(AuthenticationResponse.error(401, "not authorized"));
        }
        String clientId = getClientId(endpoint);
        String userName = endpoint.auth().getUsername();
        String passWord = endpoint.auth().getPassword();
        MqttAuthenticationRequest request = new MqttAuthenticationRequest(clientId, userName, passWord, getTransport());

        return registry
                .getDevice(clientId)
                .flatMap(operator -> operator.authenticate(request));

    }

    protected Mono<MqttDeviceSession> doAccept(String deviceId, MqttEndpoint endpoint) {
        return registry
                .getDevice(deviceId)
                .switchIfEmpty(Mono.error(() -> new NullPointerException("device not found")))
                .doOnError(err -> {
                    logger.error("get device operator error", err);
                    endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                })
                .flatMap(operator -> accept(endpoint, new MqttDeviceSession(deviceId, getTransport(), endpoint, operator)));

    }

    private Mono<MqttDeviceSession> doConnect(MqttEndpoint endpoint) {
        return Mono.defer(() -> {
            if (deviceSessionManager.isOutOfMaximumSessionLimit(getTransport())) {
                //当前连接超过了最大连接数
                logger.warn("reject mqtt connection[{}],out of maximum session limits:[{}]!",
                        endpoint.clientIdentifier(),
                        deviceSessionManager.getMaximumSession(getTransport()));

                endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                return Mono.empty();
            }

            String clientId = getClientId(endpoint);
            //进行认证
            return doAuth(endpoint)
                    .doOnError(error -> {
                        logger.warn("device[{}] auth error", clientId, error);
                        endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                    })
                    .flatMap((response) -> {
                        if (response.isSuccess()) {
                            String deviceId = Optional.ofNullable(response.getDeviceId()).orElse(clientId);
                            return doAccept(deviceId, endpoint);
                        } else if (401 == response.getCode()) {
                            logger.info("device [{}] auth error:{}", clientId, response.getMessage());
                            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                        } else {
                            logger.warn("device [{}] auth error:{}", clientId, response);
                            endpoint.reject(MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                        }
                        return Mono.empty();
                    })
                    .switchIfEmpty(Mono.create((sink) -> {

                        serverContext
                                .doUnknownMqttClientConnection(new VertxMqttClientConnection(endpoint, deviceId -> doAccept(deviceId, endpoint)
                                        .doOnNext(sink::success)
                                        .doOnError(sink::error)
                                        .doOnCancel(sink::success)
                                        .thenReturn(true)));
                    }));
        });

    }

    protected void doCloseEndpoint(MqttEndpoint client, String deviceId) {
        logger.debug("close [{}] mqtt connection", deviceId);
        DeviceSession old = deviceSessionManager.unregister(deviceId);
        if (old == null) {
            if (client.isConnected()) {
                client.close();
            }
        }
    }

    protected Mono<MqttDeviceSession> accept(MqttEndpoint endpoint, MqttDeviceSession session) {
        return Mono.defer(() -> {
            String deviceId = session.getDeviceId();
            //注册
            deviceSessionManager.register(session);
            endpoint
                    //SUBSCRIBE
                    .subscribeHandler(subscribe -> serverContext.doSubscribe(session, subscribe))
                    //UNSUBSCRIBE
                    .unsubscribeHandler(unsubscribe -> serverContext.doUnSubscribe(session, unsubscribe))
                    //QoS 1 PUBACK
                    .publishAcknowledgeHandler(messageId -> serverContext.doAck(session, AckType.PUBACK, messageId))
                    //QoS 2  PUBREC
                    .publishReceivedHandler(messageId -> serverContext.doAck(session, AckType.PUBREC, messageId))
                    //QoS 2  PUBREL
                    .publishReleaseHandler(messageId -> serverContext.doAck(session, AckType.PUBREC, messageId))
                    //QoS 2  PUBCOMP
                    .publishCompletionHandler(messageId -> serverContext.doAck(session, AckType.PUBCOMP, messageId))
                    //断开连接 DISCONNECT
                    .disconnectHandler(v -> {
                        logger.debug("mqtt client[{}] disconnect", deviceId);
                        doCloseEndpoint(endpoint, deviceId);
                    })
                    //接收客户端推送的消息
                    .publishHandler(message -> handleMqttPublishMessage(session, endpoint, message))
                    .exceptionHandler(e -> {
                        logger.debug("mqtt client [{}] error", deviceId, e);
                        doCloseEndpoint(endpoint, deviceId);
                    })
                    .closeHandler(v -> doCloseEndpoint(endpoint, deviceId))
                    .accept(false);

            MqttWill will = endpoint.will();
            if (will != null && will.getWillMessageBytes() != null) {
                handleWillMessage(session, endpoint, will);
            }
            return Mono.just(session);
        }).doOnError(err -> {
            logger.error("accept connection error", err);
            doCloseEndpoint(endpoint, session.getDeviceId());
        });
    }


    protected void handleMqttMessage(DeviceSession session, MqttEndpoint endpoint, MqttMessage message) {
        session.ping();
        if (logger.isDebugEnabled()) {
            logger.debug("receive device[{}] message=>{}", session.getId(), message);
        }
        messageHandler
                .handleMessage(session.getOperator(), getTransport(), FromDeviceMessageContext.of(session, message))
                .switchIfEmpty(Mono.error(() -> new UnsupportedOperationException("cannot decode message")))
                .doOnError(error ->
                        logger.error("handle device[{}] message error :\n{}",
                                session.getDeviceId(),
                                message,
                                error))
                .subscribe(success -> {
                    if (success) {
                        if (message.getQosLevel() == MqttQoS.AT_LEAST_ONCE.value()) {
                            endpoint.publishAcknowledge(message.getMessageId());
                        } else if (message.getQosLevel() == MqttQoS.EXACTLY_ONCE.value()) {
                            endpoint.publishReceived(message.getMessageId());
                        }
                    }
                });
    }

    protected void handleWillMessage(DeviceSession session, MqttEndpoint endpoint, MqttWill message) {
        handleMqttMessage(session, endpoint, new VertxMqttWillMessage(session.getDeviceId(), message));
    }

    protected void handleMqttPublishMessage(DeviceSession session, MqttEndpoint endpoint, MqttPublishMessage message) {
        handleMqttMessage(session, endpoint, new VertxMqttMessage(session.getDeviceId(), message));
    }
}
