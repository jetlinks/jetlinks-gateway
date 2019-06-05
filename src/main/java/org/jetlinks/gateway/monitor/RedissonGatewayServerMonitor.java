package org.jetlinks.gateway.monitor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.codec.Transport;
import org.redisson.api.RMap;
import org.redisson.api.RQueue;
import org.redisson.api.RedissonClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
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

    public RedissonGatewayServerMonitor(String currentServerId, RedissonClient redissonClient, ScheduledExecutorService executorService) {
        this.allServerId = redissonClient.getMap(getRedisKey("server:all"));
        this.client = redissonClient;
        this.currentServerId = currentServerId;
        this.executorService = executorService;
        current = newGatewayServerInfo(currentServerId);
        serverDownQueue = client.getQueue("device-gateway-server-down");
    }

    private GatewayServerInfo newGatewayServerInfo(String serverId) {
        return new GatewayServerInfo() {
            @Override
            public String getId() {
                return serverId;
            }

            @Override
            public List<String> getTransportHosts(Transport transport) {

                return new ArrayList<>(client.getSet(getRedisKey(transport_hosts, serverId, transport.name())));
            }

            @Override
            public List<Transport> getAllTransport() {

                return new ArrayList<>(client.getSet(getRedisKey(transport_all_support, serverId)));
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
                return client.getAtomicLong(getRedisKey(transport_connection_total, serverId, transport.name())).get();
            }

            @Override
            public Map<Transport, Long> getDeviceConnectionTotalGroup() {

                return getAllTransport()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), this::getDeviceConnectionTotal));
            }
        };
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
        }
    }

    @Override
    public synchronized void registerTransport(Transport transport, String... hosts) {
        client.getSet(getRedisKey(transport_all_support, currentServerId)).add(transport);
        client.getSet(getRedisKey(transport_hosts, currentServerId, transport.name())).addAll(Arrays.asList(hosts));
        if (!startup) {
            doStartup();
        }
    }

    @Override
    public void reportDeviceCount(Transport transport, long count) {
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

        client.getSet(getRedisKey(transport_all_support, currentServerId))
                .delete();
    }

    @PreDestroy
    public void shutdown() {
        allServerId.fastRemove(currentServerId);
        serverDownQueue.add(currentServerId);
        clean();
    }

    protected synchronized void doStartup() {
        if (startup) {
            return;
        }
        startup = true;
        allServerId.put(currentServerId, System.currentTimeMillis());
        serverDownQueue.remove(currentServerId);
        executorService.scheduleAtFixedRate(() -> {
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


    }
}
