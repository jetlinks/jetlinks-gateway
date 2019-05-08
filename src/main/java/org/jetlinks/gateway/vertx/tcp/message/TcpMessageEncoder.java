package org.jetlinks.gateway.vertx.tcp.message;

import io.vertx.core.buffer.Buffer;

public interface TcpMessageEncoder {
    Buffer encode(MessageType type, Buffer data);
}
