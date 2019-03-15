package org.jetlinks.gateway.vertx.udp;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.net.SocketAddress;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.Transport;

import java.util.concurrent.TimeUnit;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Getter
@Slf4j
public class UDPDeviceSession implements DeviceSession {
    private String id;

    private String deviceId;

    private long lastPingTime = System.currentTimeMillis();

    private long connectTime = System.currentTimeMillis();

    private SocketAddress socketAddress;

    private DatagramSocket socket;

    public UDPDeviceSession(String deviceId, SocketAddress socketAddress, DatagramSocket socket) {
        this.deviceId = deviceId;
        this.id = deviceId;
        this.socketAddress = socketAddress;
        this.socket = socket;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public long lastPingTime() {
        return lastPingTime;
    }

    @Override
    public long connectTime() {
        return connectTime;
    }

    @Override
    public void send(EncodedMessage encodedMessage) {
        socket.send(Buffer.buffer(encodedMessage.getByteBuf()),
                socketAddress.port(),
                socketAddress.host(), result -> {
                    if (result.succeeded()) {
                        ping();
                    } else {
                        log.error("向UDP客户端:[{}]发送数据失败", deviceId, result.cause());
                    }
                });
    }

    @Override
    public Transport getTransport() {
        return Transport.UDP;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (Exception e) {
            log.error("关闭客户端[{}]连接失败", getId(), e);
        }
    }

    @Override
    public void ping() {
        lastPingTime = System.currentTimeMillis();
    }

    @Override
    public boolean isAlive() {
        //1分钟没有ping则认为失效
        return System.currentTimeMillis() - lastPingTime <= TimeUnit.MINUTES.toMillis(1);
    }
}
