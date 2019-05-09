package org.jetlinks.gateway.vertx.udp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.net.SocketAddress;
import lombok.SneakyThrows;
import org.jetlinks.coap.CoapPacket;
import org.jetlinks.coap.enums.Code;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class CoAPServerTest {

    private Vertx vertx = Vertx.vertx();

    public void startServer(int port, Consumer<Boolean> whenStart, Consumer<CoapPacket> consumer) {
        CoAPServer coAPServer = new CoAPServer() {
            @Override
            protected UDPDeviceSession getDevice(SocketAddress address, CoapPacket packet) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected void handleCoAPMessage(SocketAddress address, CoapPacket packet) {
                consumer.accept(packet);
            }
        };
        coAPServer.setPort(port);

        coAPServer.setOptions(new DatagramSocketOptions());

        vertx.deployVerticle(coAPServer, result -> {
            whenStart.accept(result.succeeded());

        });
    }

    @SneakyThrows
    @Test
    public void testCoap() {
        int port = 5050;
        CountDownLatch startCountDown = new CountDownLatch(1);

        AtomicReference<CoapPacket> packetReference = new AtomicReference<>();

        AtomicReference<CountDownLatch> messageCountDown = new AtomicReference<>();

        startServer(port, success -> startCountDown.countDown(), coapPacket -> {
            packetReference.set(coapPacket);
            messageCountDown.get().countDown();
        });

        Assert.assertTrue(startCountDown.await(5, TimeUnit.SECONDS));
//        DatagramSocket socket = new DatagramSocket();
//        socket.connect(new InetSocketAddress("127.0.0.1", port));
        for (int i = 0; i < 10000; i++) {
            messageCountDown.set(new CountDownLatch(1));
            String payload = "testmessage" + i;

            CoapPacket coapPacket = new CoapPacket();
            coapPacket.setCode(Code.C205_CONTENT);
            coapPacket.setMessageId(i);
            coapPacket.setPayload(payload);

            ByteBuf byteBuf=Unpooled.buffer();
            coapPacket.writeTo(new ByteBufOutputStream(byteBuf));

            vertx.createDatagramSocket()
                    .sender(port,"127.0.0.1")
                    .write(Buffer.buffer(byteBuf))
                    .end();

//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//
//            coapPacket.writeTo(outputStream);
//
//            socket.send(new DatagramPacket(outputStream.toByteArray(), 0, outputStream.size()));

            Assert.assertTrue(messageCountDown.get().await(5, TimeUnit.SECONDS));
            Assert.assertNotNull(packetReference.get());

            Assert.assertEquals(packetReference.get().getMessageId(), i);
            Assert.assertEquals(packetReference.get().getPayloadString(), payload);
            packetReference.set(null);
        }

//        socket.close();
    }

}