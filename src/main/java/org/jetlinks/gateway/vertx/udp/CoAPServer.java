package org.jetlinks.gateway.vertx.udp;

import io.netty.buffer.ByteBufInputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.net.SocketAddress;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.coap.CoapPacket;
import org.jetlinks.coap.exception.CoapException;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.EmptyDeviceMessage;
import org.jetlinks.core.message.codec.CoAPMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.gateway.session.DeviceSession;
import org.jetlinks.gateway.session.DeviceSessionManager;

import java.util.function.BiConsumer;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
@Getter
@Setter
public abstract class CoAPServer extends UDPServer {

    protected DeviceRegistry deviceRegistry;

    protected DeviceSessionManager sessionManager;

    protected ProtocolSupports protocolSupports;

    protected BiConsumer<DeviceSession, DeviceMessage> messageConsumer;

    @Override
    protected void handleUDPMessage(DatagramPacket packet) {
        SocketAddress sender = packet.sender();
        Buffer buffer = packet.data();
        try {
            CoapPacket coapPacket = CoapPacket.deserialize(new ByteBufInputStream(buffer.getByteBuf()));
            handleCoAPMessage(sender, coapPacket);
        } catch (CoapException e) {
            log.error("解析CoAP[{}:{}]消息失败:{}", sender.host(), sender.port(), buffer.toString(), e);
        } catch (Exception e) {
            log.error("处理CoAP[{}:{}]消息失败:{}", sender.host(), sender.port(), buffer.toString(), e);
        }
    }

    protected abstract UDPDeviceSession getDevice(SocketAddress address, CoapPacket packet);

    protected void unregisterClient(String idOrDeviceId) {
        sessionManager.unregister(idOrDeviceId);
    }

    protected void handleCoAPMessage(SocketAddress address, CoapPacket packet) {
        if (log.isInfoEnabled()) {
            log.info("接受到CoAP消息: [{}:{}] {}", address.host(),
                    address.port(),
                    packet.toString(true, false, true, false));
        }
        UDPDeviceSession session = getDevice(address, packet);
        if (session != null) {
            session.ping();
            DeviceOperation deviceOperation = session.getOperation();
            CoAPMessage coapMessage = new CoAPMessage(session.getDeviceId(), packet);
            DeviceMessage message = deviceOperation.getProtocol()
                    .getMessageCodec()
                    .decode(Transport.CoAP, new FromDeviceMessageContext() {

                        @Override
                        public DeviceOperation getDeviceOperation() {
                            return deviceOperation;
                        }

                        @Override
                        public void sendToDevice(EncodedMessage message) {
                            session.send(message);
                        }

                        @Override
                        public void disconnect() {
                            unregisterClient(session.getDeviceId());
                        }

                        @Override
                        public EncodedMessage getMessage() {
                            return coapMessage;
                        }

                    });
            if (message == null || message instanceof EmptyDeviceMessage) {
                return;
            }
            if (message instanceof DeviceMessageReply) {
                sessionManager.handleDeviceMessageReply(session, ((DeviceMessageReply) message));
            }
            messageConsumer.accept(session, message);
        }
    }


}
