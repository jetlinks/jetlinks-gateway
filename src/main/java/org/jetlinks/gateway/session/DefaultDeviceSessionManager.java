package org.jetlinks.gateway.session;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.gateway.monitor.GatewayServerMonitor;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.device.DeviceInfo;
import org.jetlinks.protocol.device.DeviceOperation;
import org.jetlinks.protocol.device.DeviceState;
import org.jetlinks.protocol.exception.ErrorCode;
import org.jetlinks.protocol.message.ChildDeviceMessage;
import org.jetlinks.protocol.message.CommonDeviceMessageReply;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.DeviceMessageReply;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.MessageEncodeContext;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.protocol.message.function.FunctionInvokeMessage;
import org.jetlinks.protocol.message.function.FunctionInvokeMessageReply;
import org.jetlinks.protocol.message.property.ReadPropertyMessage;
import org.jetlinks.protocol.message.property.ReadPropertyMessageReply;
import org.jetlinks.protocol.metadata.FunctionMetadata;
import org.jetlinks.protocol.utils.IdUtils;
import org.jetlinks.registry.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Optional.*;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class DefaultDeviceSessionManager implements DeviceSessionManager {

    private Map<String, DeviceSession> repository = new ConcurrentHashMap<>(256);

    @Getter
    @Setter
    private Logger log = LoggerFactory.getLogger(DefaultDeviceSessionManager.class);

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
    private GatewayServerMonitor gatewayServerMonitor;

    @Getter
    @Setter
    private ProtocolSupports protocolSupports;

    @Getter
    @Setter
    private ScheduledExecutorService executorService;

    @Getter
    @Setter
    private Consumer<DeviceSession> onDeviceRegister;

    @Getter
    @Setter
    private Consumer<DeviceSession> onDeviceUnRegister;

    private Queue<Runnable> closeClientJobs = new ArrayDeque<>();

    private LongAdder counter = new LongAdder();

    private Map<Transport, LongAdder> transportCounter = new ConcurrentHashMap<>();

    @Getter
    @Setter
    private Map<Transport, Long> transportLimits = new ConcurrentHashMap<>();

    public void setTransportLimit(Transport transport, long limit) {
        transportLimits.put(transport, limit);
    }

    @Override
    public long getMaximumConnection(Transport transport) {
        return ofNullable(transportLimits.get(transport)).orElse(Long.MAX_VALUE);
    }

    @Override
    public long getCurrentConnection(Transport transport) {
        return ofNullable(transportCounter.get(transport))
                .map(LongAdder::longValue)
                .orElse(0L);
    }

    @Override
    public boolean isOutOfMaximumConnectionLimit(Transport transport) {

        return getCurrentConnection(transport) >= getMaximumConnection(transport);
    }

    public void shutdown() {
        new ArrayList<>(repository.values())
                .stream()
                .map(DeviceSession::getId)
                .forEach(this::unregister);
    }

    protected void doSend(DeviceMessage message, DeviceSession session) {
        String deviceId = message.getDeviceId();
        DeviceOperation operation = deviceRegistry.getDevice(deviceId);
        //获取协议并转码
        EncodedMessage encodedMessage = protocolSupports
                .getProtocol(operation.getDeviceInfo().getProtocol())
                .getMessageCodec()
                .encode(session.getTransport(), new MessageEncodeContext() {
                    @Override
                    public DeviceMessage getMessage() {
                        return message;
                    }

                    @Override
                    public DeviceOperation getDeviceOperation() {
                        return operation;
                    }
                });
        //发往设备
        session.send(encodedMessage);
        //如果是异步操作，则直接返回结果
        if (message instanceof FunctionInvokeMessage) {
            FunctionInvokeMessage invokeMessage = ((FunctionInvokeMessage) message);
            boolean async = Boolean.TRUE.equals(invokeMessage.getAsync())
                    || session.getOperation()
                    .getMetadata()
                    .getFunction(invokeMessage.getFunctionId())
                    .map(FunctionMetadata::isAsync)
                    .orElse(false);
            if (async) {
                //直接回复消息
                deviceMessageHandler.reply(FunctionInvokeMessageReply.builder()
                        .messageId(message.getMessageId())
                        .deviceId(deviceId)
                        .message(ErrorCode.REQUEST_HANDLING.getText())
                        .code(ErrorCode.REQUEST_HANDLING.name())
                        .success(false)
                        .build());
            }
        }
    }

    public void handleDeviceMessageReply(DeviceSession session, DeviceMessageReply reply) {
        if (reply instanceof FunctionInvokeMessageReply) {
            FunctionInvokeMessageReply message = ((FunctionInvokeMessageReply) reply);
            //判断是否为异步操作，如果不异步的，则需要同步回复结果
            boolean async = session.getOperation()
                    .getMetadata()
                    .getFunction(message.getFunctionId())
                    .map(FunctionMetadata::isAsync)
                    .orElse(false);
            //同步操作则直接返回
            if (!async) {
                if (StringUtils.isEmpty(message.getMessageId())) {
                    log.warn("消息无messageId:{}", message.toJson());
                    return;
                }
                deviceMessageHandler.reply(message);
            }
        }

    }

    protected DeviceMessage createChildDeviceMessage(DeviceOperation childDevice, DeviceMessage message) {
        ChildDeviceMessage deviceMessage = new ChildDeviceMessage();
        DeviceInfo deviceInfo = childDevice.getDeviceInfo();

        deviceMessage.setChildDeviceId(deviceInfo.getId());
        deviceMessage.setChildDeviceMessage(message);
        deviceMessage.setDeviceId(deviceInfo.getParentDeviceId());
        deviceMessage.setMessageId(IdUtils.newUUID());

        return deviceMessage;
    }

    public DeviceMessageReply createChildDeviceMessageReply(DeviceMessage source, Object reply) {
        CommonDeviceMessageReply messageReply = null;
        if (source instanceof ReadPropertyMessage) {
            if (reply == null || reply instanceof ErrorCode) {
                messageReply = new ReadPropertyMessageReply();
                messageReply.setMessageId(source.getMessageId());
                messageReply.setDeviceId(source.getDeviceId());
            } else if (reply instanceof ReadPropertyMessageReply) {
                messageReply = ((ReadPropertyMessageReply) reply);
            }

        } else if (source instanceof FunctionInvokeMessage) {
            if (reply == null || reply instanceof ErrorCode) {
                messageReply = new FunctionInvokeMessageReply();
                messageReply.setMessageId(source.getMessageId());
                messageReply.setDeviceId(source.getDeviceId());
            } else if (reply instanceof FunctionInvokeMessageReply) {
                messageReply = ((FunctionInvokeMessageReply) reply);
            }
        } else if (reply instanceof DeviceMessageReply) {
            return (DeviceMessageReply) reply;
        } else {
            if (reply == null || reply instanceof ErrorCode) {
                messageReply = new CommonDeviceMessageReply();
                messageReply.setMessageId(source.getMessageId());
                messageReply.setDeviceId(source.getDeviceId());
            } else {
                throw new UnsupportedOperationException("不支持的消息[" + reply.getClass() + "]:" + reply);
            }
        }
        if (reply instanceof ErrorCode) {
            messageReply.error((ErrorCode) reply);
        }
        return messageReply;
    }

    public void init() {
        deviceMessageHandler.handleDeviceCheck(serverId, deviceId -> {
            DeviceSession session = repository.get(deviceId);
            DeviceOperation operation = deviceRegistry.getDevice(deviceId);
            if (session == null) {
                if (serverId.equals(operation.getServerId())) {
                    operation.offline();
                }
            } else {
                operation.putState(DeviceState.online);
            }
        });
        //接收发往设备的消息
        deviceMessageHandler.handleMessage(serverId, message -> {
            String deviceId = message.getDeviceId();
            DeviceSession session = repository.get(deviceId);
            //直连设备
            if (session != null) {
                doSend(message, session);
            } else {
                DeviceOperation operation = deviceRegistry.getDevice(deviceId);
                String parentId = operation.getDeviceInfo().getParentDeviceId();
                if (null != parentId && !parentId.isEmpty()) {
                    DeviceMessage childMessage = createChildDeviceMessage(operation, message);
                    session = repository.get(parentId);
                    //网关设备就在当前服务器
                    if (session != null) {
                        doSend(childMessage, session);
                    } else {
                        DeviceMessageReply reply;
                        try {
                            //向父设备发送消息
                            reply = deviceRegistry.getDevice(parentId)
                                    .messageSender()
                                    .send(childMessage, obj -> createChildDeviceMessageReply(message, obj))
                                    .toCompletableFuture()
                                    .get(30, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            reply = createChildDeviceMessageReply(message, null);
                            reply.error(ErrorCode.TIME_OUT);
                        } catch (Exception e) {
                            log.error("等待子设备返回消息失败", e);
                            reply = createChildDeviceMessageReply(message, null);
                            reply.error(ErrorCode.SYSTEM_ERROR);
                        }
                        deviceMessageHandler.reply(reply);
                    }
                } else {
                    //设备不在当前服务器节点
                    log.warn("设备[{}]未连接服务器[{}],无法发送消息:{}", deviceId, serverId, message.toJson());
                    DeviceMessageReply reply = createChildDeviceMessageReply(message, null);
                    reply.error(ErrorCode.CLIENT_OFFLINE);
                    deviceMessageHandler.reply(reply);
                }
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
            for (Map.Entry<Transport, LongAdder> entry : transportCounter.entrySet()) {
                //提交当前节点的监控
                gatewayServerMonitor.reportDeviceCount(entry.getKey(), entry.getValue().longValue());
            }

            log.debug("当前节点设备连接数量:{},本次检查失效设备数量:{},集群中总连接设备数量:{}",
                    transportCounter, closed, gatewayServerMonitor.getDeviceCount());

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
    public DeviceSession register(DeviceSession session) {
        DeviceSession old = repository.put(session.getDeviceId(), session);
        if (null != old) {
            old.close();
        } else {
            transportCounter
                    .computeIfAbsent(session.getTransport(), transport -> new LongAdder())
                    .add(1);
            counter.add(1);
        }
        if (!session.getId().equals(session.getDeviceId())) {
            repository.put(session.getId(), session);
        }
        deviceRegistry
                .getDevice(session.getDeviceId())
                .online(serverId, session.getId());
        if (null != onDeviceRegister) {
            onDeviceRegister.accept(session);
        }
        return old;
    }

    @Override
    public DeviceSession unregister(String idOrDeviceId) {
        DeviceSession client = repository.remove(idOrDeviceId);

        if (null != client) {
            transportCounter
                    .computeIfAbsent(client.getTransport(), transport -> new LongAdder())
                    .add(-1);
            counter.add(-1);
            if (!client.getId().equals(client.getDeviceId())) {
                repository.remove(client.getId().equals(idOrDeviceId) ? client.getDeviceId() : client.getId());
            }
            closeClientJobs.add(client::close);
            deviceRegistry.getDevice(client.getDeviceId()).offline();
            if (null != onDeviceUnRegister) {
                onDeviceUnRegister.accept(client);
            }
        }
        return client;
    }

}
