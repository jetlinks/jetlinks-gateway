package org.jetlinks.gateway.vertx.udp;

import io.netty.buffer.ByteBufInputStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.net.SocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.coap.CoapPacket;
import org.jetlinks.coap.exception.CoapException;
import org.jetlinks.protocol.message.codec.CoAPMessage;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.FromDeviceMessageContext;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.protocol.metadata.DeviceMetadata;
import org.jetlinks.registry.api.DeviceInfo;
import org.jetlinks.registry.api.DeviceOperation;

import java.net.InetSocketAddress;

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
            CoapPacket coapPacket = CoapPacket.deserialize(new InetSocketAddress(sender.host(), sender.port()), new ByteBufInputStream(buffer.getByteBuf()));
            handleCoAPMessage(sender, coapPacket);
        } catch (CoapException e) {
            log.error("解析CoAP[{}:{}]消息失败:{}", sender.host(), sender.port(), buffer.toString(), e);
        } catch (Exception e) {
            log.error("处理CoAP[{}:{}]消息失败:{}", sender.host(), sender.port(), buffer.toString(), e);
        }
    }

    protected abstract DeviceOperation getDevice(SocketAddress address, CoapPacket packet);

    protected void unregisterClient(String idOrDeviceId) {
        sessionManager.unregister(idOrDeviceId);
    }

    protected void handleCoAPMessage(SocketAddress address, CoapPacket packet) {
        DeviceOperation deviceOperation = getDevice(address, packet);
        if (deviceOperation != null) {
            DeviceInfo deviceInfo = deviceOperation.getDeviceInfo();
            String protocol = deviceInfo.getProtocol();
            CoAPMessage coapMessage = new CoAPMessage(deviceInfo.getId(), packet);
            protocolSupports.getProtocol(protocol)
                    .getMessageCodec()
                    .decode(Transport.Coap, new FromDeviceMessageContext() {
                        @Override
                        public void sendToDevice(EncodedMessage message) {
                            socket.send(Buffer.buffer(message.getByteBuf()), address.port(), host, result -> {
                                if (!result.succeeded()) {
                                    log.error("发送CoAP消息失败:{}", message.toString(), result.cause());
                                } else if (log.isDebugEnabled()) {
                                    log.info("发送CoAP消息成功:{}", message.toString());
                                }
                            });
                        }

                        @Override
                        public void disconnect() {
                            unregisterClient(deviceInfo.getId());
                        }

                        @Override
                        public EncodedMessage getMessage() {
                            return coapMessage;
                        }

                        @Override
                        public DeviceMetadata getDeviceMetadata() {
                            return deviceOperation.getMetadata();
                        }
                    });
        }
    }


}
