package org.jetlinks.gateway.session;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.device.DeviceOperation;

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
