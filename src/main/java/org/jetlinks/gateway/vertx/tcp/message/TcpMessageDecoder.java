package org.jetlinks.gateway.vertx.tcp.message;

import io.vertx.core.buffer.Buffer;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface TcpMessageDecoder {
    void decode(Buffer data);

    TcpMessageDecoder handlerPing(Consumer<Void> consumer);

    TcpMessageDecoder handlerMessage(BiConsumer<MessageType, Buffer> consumer);

    TcpMessageDecoder handlerUnKnownMessage(Consumer<Byte> consumer);
}
