package org.jetlinks.gateway.vertx.tcp.message;

/**
 * TCP消息编解码器,用于tcp消息的初步解码
 *
 * @see org.jetlinks.gateway.vertx.tcp.fixed.FixedLengthTcpCodec
 */
public interface TcpMessageCodec {

    TcpMessageDecoder decoder();

    TcpMessageEncoder encoder();


}
