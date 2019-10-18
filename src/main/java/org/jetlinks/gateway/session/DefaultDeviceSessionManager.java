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
import org.jetlinks.core.message.codec.EmptyMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.ToDeviceMessageContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.core.message.event.EventMessage;
import org.jetlinks.core.utils.IdUtils;
import org.jetlinks.gateway.monitor.GatewayServerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
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

    private String serverId;

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
        DeviceMessageReply reply;
        if (message instanceof RepayableDeviceMessage) {
            reply = ((RepayableDeviceMessage) message).newReply();
        } else {
            reply = new CommonDeviceMessageReply();
        }
        reply.messageId(message.getMessageId()).deviceId(deviceId);

        if (message instanceof DisconnectDeviceMessage) {
            unregister(session.getId());
            deviceMessageHandler.reply(reply.success())
                    .whenComplete((success, error) -> {
                        if (error != null) {
                            log.error("回复断开连接失败: {}", reply, error);
                        }
                    });
            return;
        } else {

            try {
                //获取协议并转码
                EncodedMessage encodedMessage = session.getProtocolSupport()
                        .getMessageCodec()
                        .encode(session.getTransport(), new ToDeviceMessageContext() {
                            @Override
                            public void sendToDevice(EncodedMessage message) {
                                session.send(message);
                            }

                            @Override
                            public void disconnect() {
                                unregister(deviceId);
                            }

                            @Override
                            public DeviceMessage getMessage() {
                                return message;
                            }

                            @Override
                            public DeviceOperation getDeviceOperation() {
                                return deviceRegistry.getDevice(deviceId);
                            }
                        });
                if (encodedMessage == null || encodedMessage instanceof EmptyMessage) {
                    return;
                }
                //直接发往设备
                session.send(encodedMessage);
                reply.message(ErrorCode.REQUEST_HANDLING.getText())
                        .code(ErrorCode.REQUEST_HANDLING.name())
                        .success();
            } catch (Throwable e) {
                reply.error(e);
            }
        }


        //如果是异步消息,先直接回复处理中...
        if (Headers.async.get(message).asBoolean().orElse(false)) {

            //直接回复消息
            deviceMessageHandler.reply(reply)
                    .whenComplete((success, error) -> {
                        if (error != null) {
                            log.error("回复异步设备消息处理中失败: {}", reply, error);
                        }
                    });
        }
    }

    private CompletionStage<Boolean> doReply(DeviceMessageReply reply) {
        return deviceMessageHandler
                .reply(reply)
                .whenComplete((success, error) -> {
                    if (error != null) {
                        log.error("回复设备消息失败:{}", reply, error);
                    }
                });
    }

    public void handleDeviceMessageReply(DeviceSession session, DeviceMessageReply reply) {
        if (StringUtils.isEmpty(reply.getMessageId())) {
            log.warn("消息无messageId:{}", reply.toJson());
            return;
        }
        //强制回复
        if (Headers.forceReply.get(reply).asBoolean().orElse(false)) {
            doReply(reply);
            return;
        }
        //支持异步的消息
        if (Headers.asyncSupport.get(reply).asBoolean().orElse(false)) {
            //reply没有标记为异步,则从消息处理器中判断是否异步
            if (!Headers.async.get(reply).asBoolean().orElse(false)) {
                //判断是否标记为异步消息,如果是异步消息,则不需要回复.
                deviceMessageHandler
                        .messageIsAsync(reply.getMessageId()) //在消息发送端,如果是异步消息,应该对消息进行标记
                        .whenComplete((async, throwable) -> {
                            //如果是同步操作则返回
                            if (!Boolean.TRUE.equals(async)) {
                                doReply(reply);
                            } else if (log.isDebugEnabled()) {
                                log.debug("收到异步消息回复:{}", reply);
                            }
                        });
                return;
            }
        }
        if (!(reply instanceof EventMessage)) {
            doReply(reply);
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
        Objects.requireNonNull(deviceRegistry, "deviceRegistry");
        Objects.requireNonNull(deviceMessageHandler, "deviceMessageHandler");
        Objects.requireNonNull(gatewayServerMonitor, "gatewayServerMonitor");
        Objects.requireNonNull(executorService, "executorService");

        serverId = gatewayServerMonitor.getCurrentServerInfo().getId();

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
            //  Map<Transport, LongAdder> real = new ConcurrentHashMap<>();

            List<String> notAliveClients = repository.values()
                    .parallelStream()
                    .filter(session -> {
                        if (session.isAlive()) {
                            //real.computeIfAbsent(session.getTransport(), (__) -> new LongAdder()).increment();
                            //检查注册中心的信息是否与当前服务器一致
                            //在redis集群宕机的时候,刚好往设备发送消息,可能导致注册中心认为设备已经离线.
                            //让设备重新上线,否则其他服务无法往此设备发送消息.
                            if (!serverId.equals(session.getOperation().getServerId())) {
                                log.warn("设备[{}]状态不正确!", session.getDeviceId());
                                session.getOperation().online(serverId, session.getId());
                            }
                            return false;
                        }
                        return true;
                    })
                    .map(DeviceSession::getId)
                    .collect(Collectors.toList());

            long closed = notAliveClients.size();

            notAliveClients.forEach(this::unregister);

//            //更新真实数量
//            transportCounter.forEach((transport, realNumber) ->
//                    Optional.ofNullable(real.get(transport))
//                            .ifPresent(counter -> realNumber.set(counter.longValue())));

            gatewayServerMonitor.getCurrentServerInfo()
                    .getAllTransport()
                    .forEach(transport -> gatewayServerMonitor.reportDeviceCount(transport,
                            Optional.ofNullable(transportCounter.get(transport))
                                    .map(LongAdder::longValue)
                                    .orElse(0L)));

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
        if (old != null) {
            //清空sessionId不同
            if (!old.getId().equals(old.getDeviceId())) {
                repository.remove(old.getId());
            }
        }
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
