package org.jetlinks.gateway.session;

import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.Transport;

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

    Transport getTransport();

    void close();

    void ping();

    boolean isAlive();
}
