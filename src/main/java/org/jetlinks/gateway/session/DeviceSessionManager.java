package org.jetlinks.gateway.session;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface DeviceSessionManager {

    DeviceSession getSession(String idOrDeviceId);

    DeviceSession register(DeviceSession deviceClient);

    DeviceSession unregister(String idOrDeviceId);

}
