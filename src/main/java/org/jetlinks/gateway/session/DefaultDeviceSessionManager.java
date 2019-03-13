package org.jetlinks.gateway.session;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class DefaultDeviceSessionManager implements DeviceSessionManager {

    private Map<String, DeviceClient> repository = new ConcurrentHashMap<>();

    @Override
    public DeviceClient getClient(String clientId) {
        return repository.get(clientId);
    }

    @Override
    public DeviceClient register(DeviceClient deviceClient) {
        return repository.put(deviceClient.getClientId(), deviceClient);
    }

    @Override
    public DeviceClient unregister(String clientId) {

        return repository.remove(clientId);
    }

}
