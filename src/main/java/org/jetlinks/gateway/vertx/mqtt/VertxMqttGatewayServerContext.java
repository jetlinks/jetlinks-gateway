package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.vertx.mqtt.messages.MqttSubscribeMessage;
import io.vertx.mqtt.messages.MqttUnsubscribeMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.server.mqtt.*;
import org.jetlinks.core.server.session.DeviceSession;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import java.util.function.Function;

@Slf4j
public class VertxMqttGatewayServerContext implements MqttGatewayServerContext {

    @Getter
    @Setter
    private Transport transport;

    private final EmitterProcessor<MqttClientAck> ackProcessor = EmitterProcessor.create(false);

    private final EmitterProcessor<MqttClientConnection> unknownConnection = EmitterProcessor.create(false);

    private final EmitterProcessor<MqttSubscription> subscriptionEmitterProcessor = EmitterProcessor.create(false);

    private final EmitterProcessor<MqttUnsubscription> unsubscriptionEmitterProcessor = EmitterProcessor.create(false);

    private final EmitterProcessor<DeviceSession> sessionEmitterProcessor = EmitterProcessor.create(false);

    public void shutdown(){
//        ackProcessor.dispose();
//        unknownConnection.dispose();
//        subscriptionEmitterProcessor.dispose();
//        unsubscriptionEmitterProcessor.dispose();
//        sessionEmitterProcessor.dispose();
    }

    void doUnknownMqttClientConnection(MqttClientConnection endpoint) {
        if (!unknownConnection.hasDownstreams()) {
            endpoint.close(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED.byteValue())
                    .subscribe(success -> log.warn("close unknown mqtt client connection:{}", endpoint));
            return;
        }
        unknownConnection.onNext(endpoint);
    }

    void doSubscribe(MqttDeviceSession session, MqttSubscribeMessage subscribe) {
        session.ping();

        VertxMqttSubscription subscription = new VertxMqttSubscription(session,subscribe);
        if(subscriptionEmitterProcessor.hasDownstreams()) {
            subscriptionEmitterProcessor.onNext(subscription);
            return;
        }
        subscription.accept();
    }

    void doUnSubscribe(MqttDeviceSession session, MqttUnsubscribeMessage unsubscribe) {

        session.ping();
        VertxMqttUnsubscription unsubscription = new VertxMqttUnsubscription(session,unsubscribe);

        if(unsubscriptionEmitterProcessor.hasDownstreams()){
            unsubscriptionEmitterProcessor.onNext(unsubscription);
            return;
        }
        unsubscription.accept();
    }

    void doAck(MqttDeviceSession session, AckType type, int messageId) {
        session.ping();
        VertxMqttClientAck ack = new VertxMqttClientAck(session, type, messageId);

        if (!ackProcessor.hasDownstreams()) {
            ack.doAck();
            return;
        }

        ackProcessor.onNext(ack);

    }

    void doAccept(DeviceSession session) {
        if (log.isDebugEnabled()) {
            log.debug("accepted device[{}] connection", session.getDeviceId());
        }
        if(sessionEmitterProcessor.hasDownstreams()){
            sessionEmitterProcessor.onNext(session);
        }
    }

    @Override
    public Flux<MqttClientConnection> handleUnknownClient() {
        return unknownConnection
                .map(Function.identity());
    }

    @Override
    public Flux<MqttSubscription> onSubscribe() {
        return subscriptionEmitterProcessor
                .map(Function.identity());
    }

    @Override
    public Flux<MqttUnsubscription> onUnsubscribe() {
        return unsubscriptionEmitterProcessor
                .map(Function.identity());
    }

    @Override
    public Flux<DeviceSession> onAccept() {
        return sessionEmitterProcessor
                .map(Function.identity());
    }

    @Override
    public Flux<MqttClientAck> onAck() {
        return ackProcessor
                .map(Function.identity());
    }

}
