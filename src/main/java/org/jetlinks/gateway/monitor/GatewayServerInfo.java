package org.jetlinks.gateway.monitor;

import org.jetlinks.core.message.codec.Transport;

import java.util.List;
import java.util.Map;

public interface GatewayServerInfo {

    String getId();

    List<String> getTransportHosts(Transport transport);

    List<Transport> getAllTransport();

    long getDeviceConnectionTotal();

    long getDeviceConnectionTotal(Transport transport);

    Map<Transport, Long> getDeviceConnectionTotalGroup();


}
