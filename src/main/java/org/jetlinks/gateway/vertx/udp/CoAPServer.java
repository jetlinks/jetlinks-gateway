package org.jetlinks.gateway.vertx.udp;

import io.netty.buffer.ByteBufInputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.net.SocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.coap.CoapPacket;
import org.jetlinks.coap.exception.CoapException;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.codec.CoAPMessage;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.FromDeviceMessageContext;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.protocol.device.DeviceInfo;
import org.jetlinks.protocol.device.DeviceOperation;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public abstract class CoAPServer extends UDPServer {

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
        log.info("接受到CoAP消息:{}", address.host(), address.port(), packet.toString(true, false, true, false));
        UDPDeviceSession session = getDevice(address, packet);
        if (session != null) {
            DeviceOperation deviceOperation = session.getOperation();
            DeviceInfo deviceInfo = deviceOperation.getDeviceInfo();
            String protocol = deviceInfo.getProtocol();
            CoAPMessage coapMessage = new CoAPMessage(deviceInfo.getId(), packet);
            DeviceMessage message = protocolSupports.getProtocol(protocol)
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
                            unregisterClient(deviceInfo.getId());
                        }

                        @Override
                        public EncodedMessage getMessage() {
                            return coapMessage;
                        }

                    });
            messageConsumer.accept(session, message);
        }
    }


}
