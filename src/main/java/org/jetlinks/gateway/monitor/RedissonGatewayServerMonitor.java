package org.jetlinks.gateway.monitor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.codec.Transport;
import org.redisson.api.RMap;
import org.redisson.api.RQueue;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class RedissonGatewayServerMonitor implements GatewayServerMonitor {

    private RMap<String, Long> allServerId;

    private RedissonClient client;

    private String currentServerId;

    @Getter
    @Setter
    private long timeToLive = 5;

    private ScheduledExecutorService executorService;

    private List<Consumer<String>> downListener = new CopyOnWriteArrayList<>();

    private static final String transport_hosts = "transport_hosts";
    private static final String transport_all_support = "transport_supports";
    private static final String transport_connection_total = "transport_conn_total";

    private volatile boolean startup = false;

    public String getRedisKey(String... key) {
        return "device-gateway:" + String.join(":", key);
    }

    private GatewayServerInfo current;

    private RQueue<String> serverDownQueue;

    private Map<String, Object> localCache = new ConcurrentHashMap<>();

    private Map<String, GatewayServerInfo> infoCache = new ConcurrentHashMap<>();

    private RTopic serverChangedTopic;

    public RedissonGatewayServerMonitor(String currentServerId, RedissonClient redissonClient, ScheduledExecutorService executorService) {
        this.allServerId = redissonClient.getMap(getRedisKey("server:all"));
        this.client = redissonClient;
        this.currentServerId = currentServerId;
        this.executorService = executorService;
        current = newGatewayServerInfo(currentServerId);
        serverDownQueue = client.getQueue("device-gateway-server-down");
        serverChangedTopic = client.getTopic("device-gateway-server-changed");
    }

    private GatewayServerInfo newGatewayServerInfo(String serverId) {
        return infoCache.computeIfAbsent(serverId, _serverId -> new GatewayServerInfo() {

            @Override
            public String getId() {
                return _serverId;
            }

            @Override
            @SuppressWarnings("all")
            public List<String> getTransportHosts(Transport transport) {
                String key = getRedisKey(transport_hosts, _serverId, transport.name());
                return (List<String>) localCache.computeIfAbsent(key, _key -> new ArrayList<>(client.getSet(_key)));
            }

            @Override
            @SuppressWarnings("all")
            public List<Transport> getAllTransport() {
                String key = getRedisKey(transport_all_support, _serverId);

                return (List<Transport>) localCache.computeIfAbsent(key, _key -> new ArrayList<>(client.getSet(_key)));
            }

            @Override
            public long getDeviceConnectionTotal() {
                return getAllTransport()
                        .stream()
                        .mapToLong(this::getDeviceConnectionTotal)
                        .sum();
            }

            @Override
            public long getDeviceConnectionTotal(Transport transport) {
                return client.getAtomicLong(getRedisKey(transport_connection_total, _serverId, transport.name())).get();
            }

            @Override
            public Map<Transport, Long> getDeviceConnectionTotalGroup() {

                return getAllTransport()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), this::getDeviceConnectionTotal));
            }
        });

    }

    @Override
    public GatewayServerInfo getCurrentServerInfo() {
        return current;
    }

    @Override
    public Optional<GatewayServerInfo> getServerInfo(String serverId) {
        if (allServerId.containsKey(serverId)) {
            return Optional.of(newGatewayServerInfo(serverId));
        }

        return Optional.empty();
    }

    @Override
    public List<GatewayServerInfo> getAllServerInfo() {

        return allServerId.keySet()
                .stream()
                .map(this::newGatewayServerInfo)
                .collect(Collectors.toList());
    }

    @Override
    public void serverOffline(String serverId) {
        log.debug("device gateway server [{}] offline ", serverId);
        long number = allServerId.fastRemove(serverId);
        if (number > 0) {
            for (Consumer<String> consumer : downListener) {
                consumer.accept(serverId);
            }
            infoCache.remove(serverId);
            localCache.clear();
            serverChangedTopic.publish(serverId);
        }
    }

    @Override
    public synchronized void registerTransport(Transport transport, String... hosts) {

        client.getSet(getRedisKey(transport_all_support, currentServerId)).add(transport);
        client.getSet(getRedisKey(transport_hosts, currentServerId, transport.name())).addAll(Arrays.asList(hosts));

        if (!startup) {
            doStartup();
        }
        serverChangedTopic.publish(currentServerId);
    }

    @Override
    public void reportDeviceCount(Transport transport, long count) {
        client.<Transport>getSet(getRedisKey(transport_all_support, currentServerId)).add(transport);

        client.getAtomicLong(getRedisKey(transport_connection_total, currentServerId, transport.name()))
                .set(count);
    }

    @Override
    public void onServerDown(Consumer<String> listener) {
        downListener.add(listener);
    }

    @Override
    public long getDeviceCount(String serverId) {
        return client.<Transport>getSet(getRedisKey(transport_all_support, serverId))
                .stream()
                .mapToLong(transport -> client.getAtomicLong(getRedisKey(transport_connection_total, serverId, transport.name())).get())
                .sum();
    }

    @Override
    public long getDeviceCount() {
        return allServerId.keySet()
                .stream()
                .mapToLong(this::getDeviceCount)
                .sum();
    }

    protected void clean() {
        client.<Transport>getSet(getRedisKey(transport_all_support, currentServerId))
                .forEach(transport -> {
                    client.getSet(getRedisKey(transport_connection_total, currentServerId, transport.name())).delete();
                    client.getSet(getRedisKey(transport_hosts, currentServerId, transport.name())).delete();
                });

        client.getSet(getRedisKey(transport_all_support, currentServerId)).delete();

    }

    @PreDestroy
    public void shutdown() {
        startup=false;
        allServerId.fastRemove(currentServerId);
        serverDownQueue.add(currentServerId);
        clean();
        serverChangedTopic.publish(currentServerId);
    }

    protected synchronized void doStartup() {
        if (startup) {
            return;
        }
        startup = true;

        allServerId.put(currentServerId, System.currentTimeMillis());
        serverDownQueue.remove(currentServerId);

        serverChangedTopic.addListener(String.class, (channel, msg) -> {
            infoCache.remove(msg);
            localCache.clear();
        });

        executorService.scheduleAtFixedRate(() -> {
            if(!startup){
                return;
            }
            log.debug("device gateway server [{}] keepalive", currentServerId);
            allServerId.put(currentServerId, System.currentTimeMillis());

            allServerId.entrySet()
                    .stream()
                    .filter(e -> System.currentTimeMillis() - e.getValue() > timeToLive * 1000)
                    .map(Map.Entry::getKey)
                    .filter(Objects::nonNull)
                    .forEach(this::serverOffline);
            //触发服务下线事件
            for (String offlineServer = serverDownQueue.poll()
                 ; offlineServer != null && (!currentServerId.equals(offlineServer))
                    ; offlineServer = serverDownQueue.poll()) {
                for (Consumer<String> listener : downListener) {
                    listener.accept(offlineServer);
                }
            }
        }, 1, Math.max(1, timeToLive - 1), TimeUnit.SECONDS);
    }

    @PostConstruct
    public void startup() {
        //初始化先清空全部信息
        clean();
    }
}
