package org.jetlinks.gateway.session;

import org.jetlinks.protocol.message.codec.EncodedMessage;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface DeviceClient {
    String getId();

    String getClientId();

    long lastPingTime();

    long connectTime();

    void send(EncodedMessage encodedMessage);

    void close();

    void ping();

    boolean isAlive();
}
