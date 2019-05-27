package org.jetlinks.gateway.session;

import org.jetlinks.core.message.DeviceMessageReply;
import org.jetlinks.core.message.codec.Transport;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface DeviceSessionManager {

    DeviceSession getSession(String idOrDeviceId);

    DeviceSession register(DeviceSession deviceClient);

    DeviceSession unregister(String idOrDeviceId);

    String getServerId();

    void handleDeviceMessageReply(DeviceSession session, DeviceMessageReply reply);

    boolean isOutOfMaximumConnectionLimit(Transport transport);

    long getMaximumConnection(Transport transport);

    long getCurrentConnection(Transport transport);

}
