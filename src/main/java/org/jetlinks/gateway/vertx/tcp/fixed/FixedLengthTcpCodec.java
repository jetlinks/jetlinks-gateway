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
        int length = data.length();
        byte[] len = int2Bytes(length, 4);
        return Buffer.buffer(length + 5)
                .appendByte(type.type)
                .appendBytes(len)
                .appendBuffer(data);

    }

    public static int bytes2Int(byte[] bytes) {
        int len = bytes.length;
        int r = (bytes[len - 1] & 0xff);
        for (int i = len - 2; i >= 0; i--) {
            r = r | ((bytes[i] & 0xff) << (8 * (len - (i + 1))));
        }
        return r;
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
        MessageType messageType = null;
        RecordParser parser = RecordParser.newFixed(5);
        int totalLength;
        volatile boolean headerParsed = false;
        boolean exit;

        @Override
        public void handle(Buffer event) {
            if (exit) {
                return;
            }
            if (!headerParsed) {
                headerParsed = true;
                //第1位: 消息类型
                byte type = event.getByte(0);
                messageType = MessageType.of(type).orElse(null);
                if (messageType == null) {
                    exit = true;
                    if (unknown != null) {
                        unknown.accept(type);
                    }
                    return;
                }
                if (messageType == MessageType.PING) {
                    if (ping != null) {
                        ping.accept(null);
                    }
                    return;
                }
                byte[] len = event.getBytes(1, 5); //1-5位: 消息长度
                int payloadLength = bytes2Int(len);
                int realLength = totalLength - 5;

                if (payloadLength != realLength) {
                    log.warn("消息实际长度[{}]与标识长度[{}]不一致", realLength, payloadLength);
                    payloadLength = Math.min(realLength, payloadLength);
                    // exit = true;
                }
                parser.fixedSizeMode(payloadLength);
            } else {
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
