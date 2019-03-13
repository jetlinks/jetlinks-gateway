package org.jetlinks.gateway.session;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface DeviceSessionManager {

    DeviceClient getClient(String clientId);

    DeviceClient register(DeviceClient deviceClient);

    DeviceClient unregister(String clientId);

}
