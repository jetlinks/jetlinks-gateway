package org.jetlinks.gateway.vertx.udp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
@Getter
@Setter
public abstract class UDPServer extends AbstractVerticle {

    protected DatagramSocketOptions options;

    protected DatagramSocket socket;

    protected String host = "0.0.0.0";

    protected int port = 5080;

    @Override
    public void start() {
        Objects.requireNonNull(options);

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
