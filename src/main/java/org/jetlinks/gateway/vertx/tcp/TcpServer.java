package org.jetlinks.gateway.vertx.tcp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.session.DeviceSessionManager;
import org.jetlinks.registry.api.DeviceRegistry;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class TcpServer extends AbstractVerticle {

    @Getter
    @Setter
    private NetServerOptions options;

    @Getter
    @Setter
    private DeviceRegistry registry;

    @Getter
    @Setter
    private DeviceSessionManager deviceSessionManager;


    private Map<NetSocket, Long> waitAuthSocket = new ConcurrentHashMap<>();

    @Override
    public void start() {
        Objects.requireNonNull(options);
//        Objects.requireNonNull(registry);
//        Objects.requireNonNull(deviceSessionManager);

        vertx.createNetServer(options)
                .connectHandler(this::handleConnection)
                .listen(result -> {
                    if (result.succeeded()) {
                        int port = result.result().actualPort();
                        log.debug("TCP server started on port {}", port);
                    } else {
                        log.warn("TCP server start failed", result.cause());
                    }
                });
        vertx.setPeriodic(10000, id -> {
            waitAuthSocket.entrySet()
                    .stream()
                    .filter(e -> System.currentTimeMillis() - e.getValue() > TimeUnit.SECONDS.toMillis(10))
                    .map(Map.Entry::getKey)
                    .forEach(this::handleNotAcceptSocket);
        });

    }

    protected void handleNotAcceptSocket(NetSocket socket) {
        log.warn("客户端[{}]授权超时", socket.remoteAddress());
        socket.close();
    }


    protected abstract void handleMessage(NetSocket socket, Buffer data);


    protected void acceptConnect(TcpDeviceSession session) {
        deviceSessionManager.register(session);
        waitAuthSocket.remove(session.getSocket());

        session.getSocket().closeHandler(handler -> this.doClose(session));

        session.getSocket().exceptionHandler(throwable -> {
            log.error("设备[{}]连接异常", session, throwable);
            this.doClose(session);
        });
    }

    protected void handleConnection(NetSocket socket) {
        waitAuthSocket.put(socket, System.currentTimeMillis());
        socket.handler(data -> {
            handleMessage(socket, data);
            if (socket.writeQueueFull()) {
                socket.pause();
            }
        });
        socket.endHandler(handler -> socket.resume());
    }

    protected void doClose(DeviceSession session) {
        try {
            session.close();
        } catch (Throwable ignore) {
        } finally {
            deviceSessionManager.unregister(session.getId());
            waitAuthSocket.remove(((TcpDeviceSession) session).getSocket());
        }
    }
}
