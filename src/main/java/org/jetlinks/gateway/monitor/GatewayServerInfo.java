package org.jetlinks.gateway.monitor;

import org.jetlinks.core.message.codec.Transport;

import java.util.List;
import java.util.Map;

/**
 * 网关服务信息
 */
public interface GatewayServerInfo {

    //服务ID
    String getId();

    //访问地址
    List<String> getTransportHosts(Transport transport);

    //支持到所有协议
    List<Transport> getAllTransport();

    //获取所有设备连接数量
    long getDeviceConnectionTotal();

    //获取指定协议到设备连接数量
    long getDeviceConnectionTotal(Transport transport);

    //协议连接数量分组
    Map<Transport, Long> getDeviceConnectionTotalGroup();


}
