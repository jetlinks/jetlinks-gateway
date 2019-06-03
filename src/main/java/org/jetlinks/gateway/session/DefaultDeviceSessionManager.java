package org.jetlinks.gateway.session;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.jetlinks.core.ProtocolSupports;
import org.jetlinks.core.device.DeviceInfo;
import org.jetlinks.core.device.DeviceOperation;
import org.jetlinks.core.device.DeviceState;
import org.jetlinks.core.device.registry.DeviceMessageHandler;
import org.jetlinks.core.device.registry.DeviceRegistry;
import org.jetlinks.core.enums.ErrorCode;
import org.jetlinks.core.message.*;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MessageEncodeContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessageReply;
import org.jetlinks.core.metadata.FunctionMetadata;
import org.jetlinks.core.utils.IdUtils;
import org.jetlinks.gateway.monitor.GatewayServerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class DefaultDeviceSessionManager implements DeviceSessionManager {

    private Map<String, DeviceSession> repository = new ConcurrentHashMap<>(4096);

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

    private Queue<Runnable> scheduleJobQueue = new ArrayDeque<>();

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
        //获取协议并转码
        EncodedMessage encodedMessage = session.getProtocolSupport()
                .getMessageCodec()
                .encode(session.getTransport(), new MessageEncodeContext() {
                    @Override
                    public DeviceMessage getMessage() {
                        return message;
                    }

                    @Override
                    public DeviceOperation getDeviceOperation() {
                        return deviceRegistry.getDevice(deviceId);
                    }
                });
        //判断是否异步消息
        if (message instanceof FunctionInvokeMessage) {
            FunctionInvokeMessage invokeMessage = ((FunctionInvokeMessage) message);
            boolean async =
                    (invokeMessage.getAsync() != null && Boolean.TRUE.equals(invokeMessage.getAsync()))
                            //metadata中默认的配置
                            || session.getOperation()
                            .getMetadata()
                            .getFunction(invokeMessage.getFunctionId())
                            .map(FunctionMetadata::isAsync)
                            .orElse(false);
            if (async) {
                deviceMessageHandler.markMessageAsync(message.getMessageId());
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
        //发往设备
        session.send(encodedMessage);
    }

    public void handleDeviceMessageReply(DeviceSession session, DeviceMessageReply reply) {
        if (StringUtils.isEmpty(reply.getMessageId())) {
            log.warn("消息无messageId:{}", reply.toJson());
            return;
        }
        if (reply instanceof FunctionInvokeMessageReply) {
            FunctionInvokeMessageReply message = ((FunctionInvokeMessageReply) reply);
            //判断是否为同步操作，如果不异步的，则需要同步回复结果
            boolean sync = !deviceMessageHandler.messageIsAsync(message.getMessageId());
            //同步操作则直接返回
            if (sync) {
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

        if (reply instanceof DeviceMessageReply) {

            ((DeviceMessageReply) reply).from(source);

            return ((DeviceMessageReply) reply);
        }
        if (source instanceof RepayableDeviceMessage) {
            DeviceMessageReply deviceMessageReply = ((RepayableDeviceMessage) source).newReply();
            deviceMessageReply.from(source);

            if (reply instanceof ErrorCode) {
                deviceMessageReply.error(((ErrorCode) reply));
            }
            return deviceMessageReply;
        }

        log.warn("不支持的子设备消息回复:source:{} reply: {}", source, reply);
        CommonDeviceMessageReply error = new CommonDeviceMessageReply();
        error.from(source);
        error.error(ErrorCode.UNSUPPORTED_MESSAGE);
        return error;
    }

    public void init() {
        //处理设备状态检查
        deviceMessageHandler.handleDeviceCheck(serverId, deviceId -> {
            DeviceSession session = repository.get(deviceId);
            DeviceOperation operation = deviceRegistry.getDevice(deviceId);
            if (session == null) {
                if (serverId.equals(operation.getServerId())) {
                    log.warn("设备[{}]未连接到当前设备网关服务[{}]", deviceId, serverId);
                    operation.offline();
                }
            } else {
                operation.putState(DeviceState.online);
            }
        });
        //接收发往设备的消息
        deviceMessageHandler.handleMessage(serverId, message -> {
            String deviceId = message.getDeviceId();
            DeviceSession session = getSession(deviceId);
            //直连设备
            if (session != null) {
                doSend(message, session);
            } else {
                DeviceOperation operation = deviceRegistry.getDevice(deviceId);
                String parentId = operation.getDeviceInfo().getParentDeviceId();
                if (null != parentId && !parentId.isEmpty()) {
                    DeviceMessage childMessage = createChildDeviceMessage(operation, message);
                    session = repository.get(parentId);
                    //父设备就在当前服务器
                    if (session != null) {
                        doSend(childMessage, session);
                    } else {
                        //向父设备发送消息
                        deviceRegistry.getDevice(parentId)
                                .messageSender()
                                .send(childMessage, obj -> createChildDeviceMessageReply(message, obj))
                                .whenComplete((reply, throwable) -> {
                                    if (throwable != null) {
                                        log.error("等待子设备返回消息失败", throwable);
                                    } else {
                                        deviceMessageHandler.reply(reply);
                                    }
                                });
                    }
                } else {
                    //设备不在当前服务器节点
                    log.warn("设备[{}]未连接服务器[{}],无法发送消息:{}", deviceId, serverId, message.toJson());
                    //检查一下真实状态
                    operation.checkState();
                    DeviceMessageReply reply = createChildDeviceMessageReply(message, null);
                    reply.error(ErrorCode.CLIENT_OFFLINE);
                    deviceMessageHandler.reply(reply);
                }
            }
        });
        //每30秒检查一次设备连接情况
        executorService.scheduleAtFixedRate(() -> {
            long startTime = System.currentTimeMillis();
            List<String> notAliveClients = repository.values()
                    .parallelStream()
                    .peek(session -> {
                        //检查注册中心的信息是否与当前服务器一致
                        //在redis集群宕机的时候,刚好往设备发送消息,可能导致注册中心认为设备已经离线.
                        //让设备重新上线,否则其他服务无法往此设备发送消息.
                        if (!serverId.equals(session.getOperation().getServerId()) && session.isAlive()) {
                            log.warn("设备[{}]状态不正确!", session.getDeviceId());
                            session.getOperation().online(serverId, session.getId());
                        }
                    })
                    .filter(session -> !session.isAlive())
                    .map(DeviceSession::getId)
                    .collect(Collectors.toList());

            long closed = notAliveClients.size();

            notAliveClients.forEach(this::unregister);

            gatewayServerMonitor.getCurrentServerInfo()
                    .getAllTransport()
                    .forEach(transport -> gatewayServerMonitor
                            .reportDeviceCount(transport, Optional.ofNullable(transportCounter.get(transport)).map(LongAdder::longValue).orElse(0L)));

            //执行任务
            int jobNumber = 0;
            for (Runnable runnable = scheduleJobQueue.poll(); runnable != null; runnable = scheduleJobQueue.poll()) {
                jobNumber++;
                runnable.run();
            }

            if (log.isInfoEnabled()) {
                log.info("当前节点设备连接数量:{},当前集群中总连接数量:{}." +
                                "本次检查连接失效数量:{}," +
                                "执行任务数量:{}," +
                                "耗时:{}ms.",
                        transportCounter,
                        gatewayServerMonitor.getDeviceCount(),
                        closed,
                        jobNumber,
                        System.currentTimeMillis() - startTime);
            }
        }, 10, 30, TimeUnit.SECONDS);

    }

    @Override
    public DeviceSession getSession(String clientId) {
        DeviceSession session = repository.get(clientId);
        if (session == null || !session.isAlive()) {
            return null;
        }
        return session;
    }

    @Override
    public DeviceSession register(DeviceSession session) {
        DeviceSession old = repository.put(session.getDeviceId(), session);
        if (!session.getId().equals(session.getDeviceId())) {
            repository.put(session.getId(), session);
        }
        if (null != old) {
            //1. 可能是多个设备使用了相同的id.
            //2. 可能是同一个设备,注销后立即上线,由于种种原因,先处理了上线后处理了注销逻辑.
            log.warn("注册的设备[{}]已存在,断开旧连接:{}", old.getDeviceId(), session);
            //加入关闭连接队列
            scheduleJobQueue.add(old::close);
        } else {
            //本地计数
            transportCounter
                    .computeIfAbsent(session.getTransport(), transport -> new LongAdder())
                    .increment();
        }
        //注册中心上线
        deviceRegistry
                .getDevice(session.getDeviceId())
                .online(serverId, session.getId());
        //通知
        if (null != onDeviceRegister) {
            onDeviceRegister.accept(session);
        }
        return old;
    }

    @Override
    public DeviceSession unregister(String idOrDeviceId) {
        DeviceSession client = repository.remove(idOrDeviceId);

        if (null != client) {
            if (!client.getId().equals(client.getDeviceId())) {
                repository.remove(client.getId().equals(idOrDeviceId) ? client.getDeviceId() : client.getId());
            }
            //本地计数
            transportCounter
                    .computeIfAbsent(client.getTransport(), transport -> new LongAdder())
                    .decrement();
            //注册中心下线
            deviceRegistry.getDevice(client.getDeviceId()).offline();
            //加入关闭连接队列
            scheduleJobQueue.add(client::close);
            //通知
            if (null != onDeviceUnRegister) {
                onDeviceUnRegister.accept(client);
            }
        }
        return client;
    }

}
