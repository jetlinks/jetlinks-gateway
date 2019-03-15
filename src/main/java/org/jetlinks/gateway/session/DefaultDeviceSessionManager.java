package org.jetlinks.gateway.session;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.MessageEncodeContext;
import org.jetlinks.protocol.metadata.DeviceMetadata;
import org.jetlinks.registry.api.DeviceMessageHandler;
import org.jetlinks.registry.api.DeviceMonitor;
import org.jetlinks.registry.api.DeviceOperation;
import org.jetlinks.registry.api.DeviceRegistry;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class DefaultDeviceSessionManager implements DeviceSessionManager {

    private Map<String, DeviceSession> repository = new ConcurrentHashMap<>(256);

    @Getter
    @Setter
    private String serverId;

    @Getter
    @Setter
    private DeviceRegistry deviceRegistry;

    @Getter
    @Setter
    private DeviceMessageHandler deviceMessageHandler;

    @Getter
    @Setter
    private DeviceMonitor deviceMonitor;

    @Getter
    @Setter
    private ProtocolSupports protocolSupports;

    @Getter
    @Setter
    private ScheduledExecutorService executorService;

    private Queue<Runnable> closeClientJobs = new LinkedBlockingQueue<>();

    private AtomicInteger counter = new AtomicInteger();

    public void shutdown() {
        deviceMonitor.reportDeviceCount(serverId, 0);
        deviceMonitor.serverOffline(serverId);
        new ArrayList<>(repository.values())
                .stream()
                .map(DeviceSession::getId)
                .forEach(this::unregister);
    }

    public void init() {
        //接收发往设备的消息
        deviceMessageHandler.handleMessage(serverId, message -> {
            String deviceId = message.getDeviceId();
            DeviceSession client = repository.get(deviceId);
            if (client != null) {
                DeviceOperation operation = deviceRegistry.getDevice(deviceId);
                String protocol = operation.getDeviceInfo().getProtocol();
                //获取协议并转码
                EncodedMessage encodedMessage = protocolSupports.getProtocol(protocol)
                        .getMessageCodec()
                        .encode(client.getTransport(), new MessageEncodeContext() {
                            @Override
                            public DeviceMessage getMessage() {
                                return message;
                            }

                            @Override
                            public DeviceMetadata getDeviceMetadata() {
                                return operation.getMetadata();
                            }
                        });
                //发往设备
                client.send(encodedMessage);
            } else {
                //设备不在当前服务器节点
                log.warn("设备[{}]未链接服务器[{}],无法发送消息:{}", deviceId, serverId, message.toJson());
            }
        });
        //每30秒检查一次设备连接情况
        executorService.scheduleAtFixedRate(() -> {
            List<String> notAliveClients = repository.values()
                    .stream()
                    .filter(client -> !client.isAlive())
                    .map(DeviceSession::getId)
                    .collect(Collectors.toList());
            long closed = notAliveClients.size();

            notAliveClients.forEach(this::unregister);
            //提交监控
            deviceMonitor.reportDeviceCount(serverId, new HashSet<>(repository.values()).size());

            log.debug("当前节点设备连接数量:{},本次检查失效设备数量:{},集群中总连接设备数量:{}",
                    counter.longValue(), closed, deviceMonitor.getDeviceCount());

            //执行任务
            for (Runnable runnable = closeClientJobs.poll(); runnable != null; runnable = closeClientJobs.poll()) {
                runnable.run();
            }
        }, 10, 30, TimeUnit.SECONDS);
    }

    @Override
    public DeviceSession getSession(String clientId) {
        return repository.get(clientId);
    }

    @Override
    public DeviceSession register(DeviceSession deviceClient) {
        DeviceSession old = repository.put(deviceClient.getDeviceId(), deviceClient);
        if (null != old) {
            old.close();
        } else {
            counter.incrementAndGet();
        }
        if (!deviceClient.getId().equals(deviceClient.getDeviceId())) {
            repository.put(deviceClient.getId(), deviceClient);
        }
        deviceRegistry
                .getDevice(deviceClient.getDeviceId())
                .online(serverId, "-");
        return old;
    }

    @Override
    public DeviceSession unregister(String idOrDeviceId) {
        DeviceSession client = repository.remove(idOrDeviceId);

        if (null != client) {
            counter.decrementAndGet();
            if (!client.getId().equals(client.getDeviceId())) {
                repository.remove(client.getId().equals(idOrDeviceId) ? client.getDeviceId() : client.getId());
            }
            closeClientJobs.add(client::close);
            deviceRegistry.getDevice(client.getDeviceId()).offline();
        }
        return client;
    }

}
