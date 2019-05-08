package org.jetlinks.gateway.vertx.tcp.fixed;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.gateway.vertx.tcp.message.MessageType;
import org.jetlinks.gateway.vertx.tcp.message.TcpMessageCodec;
import org.jetlinks.gateway.vertx.tcp.message.TcpMessageDecoder;
import org.jetlinks.gateway.vertx.tcp.message.TcpMessageEncoder;

import java.math.BigInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * ------------------------------------------------------------
 * |消息类型(1 byte) | 消息长度(4 bytes) | 消息体({length} bytes)|
 * ------------------------------------------------------------
 */
@Slf4j
public class FixedLengthTcpCodec implements TcpMessageCodec, TcpMessageEncoder {

    @Override
    public TcpMessageEncoder encoder() {
        return this;
    }

    /**
     * 消息解码器
     */
    @Override
    public TcpMessageDecoder decoder() {
        return new DefaultTcpMessageDecoder();
    }

    @Override
    public Buffer encode(MessageType type, Buffer data) {
        byte[] len = int2Bytes(data.length(), 4);
        return Buffer.buffer(type.type).appendBytes(len).appendBuffer(data);

    }

    public static byte[] int2Bytes(int value, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[len - i - 1] = (byte) ((value >> 8 * i) & 0xff);
        }
        return b;
    }

    class DefaultTcpMessageDecoder implements TcpMessageDecoder, Handler<Buffer> {
        Consumer<Void> ping;
        BiConsumer<MessageType, Buffer> dataConsumer;
        Consumer<Byte> unknown;
        byte type = -1;
        RecordParser parser = RecordParser.newFixed(5);
        int totalLength;
        volatile boolean headerParsed = false;
        boolean error;

        @Override
        public void handle(Buffer event) {
            if (error) {
                return;
            }
            if (!headerParsed) {
                headerParsed = true;
                type = event.getByte(0); //第1位: 消息类型
                MessageType messageType = MessageType.of(type).orElse(null);
                if (messageType == null) {
                    error = true;
                    if (unknown != null) {
                        unknown.accept(type);
                    }
                    return;
                }
                byte[] len = event.getBytes(1, 5); //1-5位: 消息长度
                int payloadLength = new BigInteger(len).intValue();
                int realLength = totalLength - 5;

                if (payloadLength != realLength) {
                    log.warn("消息实际长度[{}]与标识长度[{}]不一致", realLength, payloadLength);
                    payloadLength = Math.min(realLength, payloadLength);
                    error = true;
                }
                parser.fixedSizeMode(payloadLength);

            } else {
                MessageType messageType = MessageType.of(type)
                        .orElse(null);

                if (messageType == MessageType.PING) {
                    if (ping != null) {
                        ping.accept(null);
                    }
                    return;
                }
                if (null != dataConsumer) {
                    dataConsumer.accept(messageType, event);
                }
            }
        }

        @Override
        public void decode(Buffer data) {
            if (headerParsed) {
                log.warn("解码器已经被使用过,此类不是线程安全的,请获取新的TcpMessageCodec.decoder()进行解码!");
                throw new IllegalStateException("不安全的操作");
            }
            this.totalLength = data.length();
            parser.handler(this);
            parser.handle(data);
        }

        @Override
        public TcpMessageDecoder handlerPing(Consumer<Void> consumer) {
            this.ping = consumer;
            return this;
        }

        @Override
        public TcpMessageDecoder handlerMessage(BiConsumer<MessageType, Buffer> consumer) {
            this.dataConsumer = consumer;
            return this;
        }

        @Override
        public TcpMessageDecoder handlerUnKnownMessage(Consumer<Byte> consumer) {
            this.unknown = consumer;
            return this;
        }
    }


}
