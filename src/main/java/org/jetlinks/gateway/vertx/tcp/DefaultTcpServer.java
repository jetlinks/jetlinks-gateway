package org.jetlinks.gateway.vertx.tcp;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.EmptyDeviceMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.vertx.tcp.message.MessageType;
import org.jetlinks.gateway.vertx.tcp.message.TcpMessageCodec;

import java.util.function.BiConsumer;

@Slf4j
public abstract class DefaultTcpServer extends TcpServer {

    @Getter
    @Setter
    private TcpMessageCodec messageCodec;

    @Getter
    @Setter
    protected ProtocolSupports protocolSupports;

    @Getter
    @Setter
    private BiConsumer<DeviceSession, DeviceMessage> deviceMessageHandler;

    protected abstract TcpAuthenticationResponse doAuth(NetSocket socket, Buffer payload);

    protected abstract void handleNoRegister(NetSocket socket);

    @Override
    protected void handleMessage(NetSocket socket, Buffer data) {
        messageCodec.decoder()
                .handlerPing(ping -> handlePing(socket))
                .handlerMessage((messageType, buffer) -> handleMessage(socket, messageType, buffer))
                .decode(data);

    }

    protected String createClientId(NetSocket socket) {
        return socket.remoteAddress()
                .toString()
                .concat("-")
                .concat(Integer.toHexString(socket.hashCode()));
    }

    protected void handlePing(NetSocket socket) {
        log.info("TCP ping from [{}] ", socket.remoteAddress());
        // String id = createClientId(socket);
    }

    protected void handleMessage(NetSocket socket, MessageType messageType, Buffer payload) {
        //授权
        if (messageType == MessageType.AUTH) {
            TcpAuthenticationResponse response = doAuth(socket, payload);
            if (response.isSuccess()) {
                TcpDeviceSession session = new TcpDeviceSession() {
                    @Override
                    public void send(EncodedMessage encodedMessage) {
                        DefaultTcpServer.this.send(socket, MessageType.MESSAGE, Buffer.buffer(encodedMessage.getByteBuf()));
                    }
                };
                session.setId(createClientId(socket));
                session.setDeviceId(response.getDeviceId());
                session.setOperationSupplier(getRegistry()::getDevice);
                session.setSocket(socket);
                acceptConnect(session);
            }
        } else { //消息
            String id = createClientId(socket);
            DeviceSession session = getDeviceSessionManager().getSession(id);
            if (null == session) {
                //设备没有注册就发送消息
                handleNoRegister(socket);
            } else {
                EncodedMessage message = EncodedMessage.simple(session.getDeviceId(), payload.getByteBuf());
                DeviceMessage deviceMessage = session.getProtocolSupport()
                        .getMessageCodec()
                        .decode(Transport.TCP, new FromDeviceMessageContext() {
                            @Override
                            public void sendToDevice(EncodedMessage message) {
                                session.send(message);
                            }

                            @Override
                            public void disconnect() {
                                doClose(session);
                            }

                            @Override
                            public EncodedMessage getMessage() {
                                return message;
                            }

                            @Override
                            public DeviceOperation getDeviceOperation() {
                                return session.getOperation();
                            }
                        });
                if (deviceMessage == null || deviceMessage instanceof EmptyDeviceMessage) {
                    return;
                }
                if (deviceMessage instanceof DeviceMessageReply) {
                    getDeviceSessionManager()
                            .handleDeviceMessageReply(session, ((DeviceMessageReply) deviceMessage));
                }
                if (null != deviceMessageHandler) {
                    deviceMessageHandler.accept(session, deviceMessage);
                }

            }
        }
    }

    protected void send(NetSocket socket, MessageType type, Buffer payload) {
        socket.write(messageCodec.encoder().encode(type, payload));
    }


}
