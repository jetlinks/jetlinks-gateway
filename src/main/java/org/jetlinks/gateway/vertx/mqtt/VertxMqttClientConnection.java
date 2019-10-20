package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.vertx.mqtt.MqttEndpoint;
import lombok.AllArgsConstructor;
import org.jetlinks.core.server.mqtt.MqttClientConnection;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class VertxMqttClientConnection implements MqttClientConnection {

    private MqttEndpoint endpoint;

    private Function<String, Mono<Boolean>> onAccept;

    public VertxMqttClientConnection(MqttEndpoint endpoint, Function<String, Mono<Boolean>> onAccept) {
        this.endpoint = endpoint;
        this.onAccept = onAccept;
    }

    private AtomicBoolean closed = new AtomicBoolean();

    private AtomicBoolean accepted = new AtomicBoolean();

    @Override
    public String getClientId() {
        return endpoint.clientIdentifier();
    }

    @Override
    public String getUsername() {
        return endpoint.auth().getUsername();
    }

    @Override
    public char[] getPassword() {
        return endpoint.auth().getPassword().toCharArray();
    }

    @Override
    public Mono<Boolean> close(byte code) {
        if (closed.get()) {
            return Mono.just(true);
        }
        return Mono.<Boolean>fromRunnable(() -> endpoint.reject(MqttConnectReturnCode.valueOf(code)))
                .thenReturn(true)
                .doOnNext(s -> closed.set(true));
    }

    @Override
    public Mono<Boolean> accept(String deviceId) {
        if (accepted.get()) {
            return Mono.just(true);
        }
        return onAccept
                .apply(deviceId)
                .doOnNext(success -> accepted.set(true));
    }
}
