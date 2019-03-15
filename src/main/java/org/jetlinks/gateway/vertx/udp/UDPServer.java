package org.jetlinks.gateway.vertx.udp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.session.DeviceSessionManager;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.registry.api.*;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
@Getter
@Setter
public abstract class UDPServer extends AbstractVerticle {

    protected DatagramSocketOptions datagramSocketOptions;

    protected DatagramSocket socket;

    protected DeviceRegistry deviceRegistry;

    protected DeviceSessionManager sessionManager;

    protected ProtocolSupports protocolSupports;

    protected BiConsumer<DeviceSession, DeviceMessage> messageConsumer;

    protected String host = "0.0.0.0";

    protected int port = 5080;

    @Override
    public void start() {
        Objects.requireNonNull(datagramSocketOptions);
        Objects.requireNonNull(deviceRegistry);
        Objects.requireNonNull(sessionManager);
        Objects.requireNonNull(protocolSupports);
        Objects.requireNonNull(messageConsumer);

        DatagramSocketOptions options = new DatagramSocketOptions();
        socket = vertx.createDatagramSocket(options);
        socket.listen(port, host, result -> {
            if (result.succeeded()) {
                DatagramSocket datagramSocket = result.result();
                log.debug("UDP server started on {}:{}", host, port);
                datagramSocket.handler(this::handleUDPMessage)
                        .exceptionHandler(err -> log.error(err.getMessage(), err))
                        .endHandler(end -> log.debug("end handle udp"));
            } else {
                log.warn("UDP server start failed", result.cause());
            }
        });
    }

    protected abstract void handleUDPMessage(DatagramPacket packet);

}
