package org.jetlinks.gateway.vertx.tcp;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import lombok.SneakyThrows;
import org.jetlinks.gateway.vertx.tcp.fixed.FixedLengthTcpCodec;
import org.jetlinks.gateway.vertx.tcp.message.MessageType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DefaultTcpServerTest {

    private Vertx vertx = Vertx.vertx();

    public DefaultTcpServer startServer(int port, Consumer<Boolean> consumer, BiConsumer<MessageType, Buffer> payloadConsumer) {
        DefaultTcpServer tcpServer = new DefaultTcpServer() {
            @Override
            protected TcpAuthenticationResponse doAuth(NetSocket socket, Buffer payload) {
                return TcpAuthenticationResponse.error(400, "不支持");
            }

            @Override
            protected void handleNoRegister(NetSocket socket) {

            }

            @Override
            protected void handleMessage(NetSocket socket, MessageType messageType, Buffer payload) {
                payloadConsumer.accept(messageType, payload);
            }
        };
        tcpServer.setAuthTimeout(TimeUnit.SECONDS.toMillis(30));
        NetServerOptions options = new NetServerOptions()
                .setPort(port)
                .setUsePooledBuffers(true)
                .setSendBufferSize(100000)
                .setReceiveBufferSize(100000);//设置最大消息长度

        tcpServer.setMessageCodec(new FixedLengthTcpCodec());

        tcpServer.setOptions(options);
        vertx.deployVerticle(tcpServer, result -> consumer.accept(result.succeeded()));
        return tcpServer;
    }

    @SneakyThrows
    @Test
    public void testSendMessage() {
        int port = 12345;

        CountDownLatch connectionCountDown = new CountDownLatch(1);
        AtomicReference<CountDownLatch> sendCountDown = new AtomicReference<>();

        AtomicReference<NetSocket> socket = new AtomicReference<>();

        AtomicReference<Buffer> payloadReference = new AtomicReference<>();
        //启动服务
        DefaultTcpServer server = startServer(port, success -> vertx
                        .createNetClient(new NetClientOptions()
                                .setUsePooledBuffers(true)
                                .setReceiveBufferSize(100000)
                                .setSendBufferSize(100000))
                        .connect(port, "127.0.0.1", result -> {
                            if (result.succeeded()) {
                                socket.set(result.result());
                            } else {
                                result.cause().printStackTrace();
                            }
                            connectionCountDown.countDown();
                        }),
                //监听消息
                (messageType, buffer) -> {
                    payloadReference.set(buffer);
                    sendCountDown.get().countDown();
                });

        connectionCountDown.await(5, TimeUnit.SECONDS);

        NetSocket client = socket.get();
        Assert.assertNotNull(client);
        for (int i = 0; i < 100000; i++) {
            try {
                sendCountDown.set(new CountDownLatch(1));
                //大字符
                StringBuilder builder = new StringBuilder();
                for (int i1 = 0; i1 < 1000; i1++) {
                    builder.append("data").append(i1).append(",");
                }
                String payload = builder.toString();

                server.send(client, MessageType.MESSAGE, Buffer.buffer(payload));

                Assert.assertTrue(sendCountDown.get().await(3, TimeUnit.SECONDS));
                Assert.assertNotNull(payloadReference.get());
                Assert.assertEquals(payloadReference.get().toString(), payload);
                payloadReference.set(null);
            } catch (Throwable e) {
                System.out.println(i);
                throw e;
            }
        }


    }

}