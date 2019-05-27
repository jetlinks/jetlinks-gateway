package org.jetlinks.gateway.monitor;

import org.jetlinks.core.message.codec.Transport;

import java.util.List;
import java.util.Optional;


/**
 * @author zhouhao
 * @since 1.0.0
 */
public interface GatewayServerMonitor {

    Optional<GatewayServerInfo> getServerInfo(String serverId);

    GatewayServerInfo getCurrentServerInfo();

    List<GatewayServerInfo> getAllServerInfo();

    void serverOffline(String serverId);

    void registerTransport(Transport transport, String... hosts);

    void reportDeviceCount(Transport transport, long count);

    long getDeviceCount(String serverId);

    long getDeviceCount();
}
