package org.jetlinks.gateway.session;

import org.jetlinks.protocol.ProtocolSupport;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.registry.api.DeviceOperation;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface DeviceSession {
    String getId();

    String getDeviceId();

    DeviceOperation getOperation();

    ProtocolSupport getProtocolSupport();

    long lastPingTime();

    long connectTime();

    void send(EncodedMessage encodedMessage);

    Transport getTransport();

    void close();

    void ping();

    boolean isAlive();
}
