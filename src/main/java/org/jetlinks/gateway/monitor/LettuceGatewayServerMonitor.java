package org.jetlinks.gateway.monitor;

import io.lettuce.core.api.StatefulRedisConnection;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.lettuce.LettucePlus;
import org.jetlinks.lettuce.RedisHaManager;
import org.jetlinks.lettuce.RedisLocalCacheMap;
import org.jetlinks.lettuce.ServerNodeInfo;
import org.jetlinks.lettuce.codec.StringCodec;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Slf4j
public class LettuceGatewayServerMonitor implements GatewayServerMonitor {

    private LettucePlus plus;

    private CurrentLettuceGatewayServerInfo current;

    private RedisHaManager haManager;

    public LettuceGatewayServerMonitor(String id, LettucePlus plus) {
        this.plus = plus;
        this.current = new CurrentLettuceGatewayServerInfo(id);
        this.haManager = plus.getHaManager("_device_gateway_server_monitor_ha");

    }

    public void startup() {
        this.haManager.onNodeLeave(nodeInfo -> serverOffline(nodeInfo.getId()));

        this.plus.<String, String>getRedisAsync(StringCodec.getInstance())
                .thenAccept(redis -> redis.sadd("d_g_s_m:all", this.current.getId()));

        this.haManager.startup(ServerNodeInfo.of(this.current.getId(), ServerNodeInfo.State.ONLINE, null));
    }

    public void shutdown() {
        plus.<String, String>getRedisAsync(StringCodec.getInstance())
                .thenAccept(redis -> redis.srem("d_g_s_m:all", this.current.getId()));
        this.current.cacheMap.clear();
        this.haManager.shutdown();
    }

    @SuppressWarnings("all")
    private class LettuceGatewayServerInfo implements GatewayServerInfo {

        @Getter
        final String id;
        String redisHashKey;
        RedisLocalCacheMap<String, Object> cacheMap;

        public LettuceGatewayServerInfo(String id) {
            this.id = id;
            this.redisHashKey = "d_g_s_m:info:".concat(id);
            this.cacheMap = plus.getLocalCacheMap(redisHashKey);
        }

        @SneakyThrows
        boolean isAlive() {
            boolean alive = plus.getConnection()
                    .thenApply(StatefulRedisConnection::async)
                    .thenCompose(redis -> redis.exists(redisHashKey))
                    .thenApply(l -> l > 0)
                    .toCompletableFuture()
                    .get();
            if (!alive) {
                cacheMap.clear();
            }
            return alive;
        }

        @Override
        public List<String> getTransportHosts(Transport transport) {
            return Optional.ofNullable(cacheMap.get("transport-host:".concat(transport.name())))
                    .map(List.class::cast)
                    .orElse(Collections.emptyList());
        }

        @Override
        @SneakyThrows
        public List<Transport> getAllTransport() {
            return Optional.ofNullable(cacheMap.get("transports"))
                    .map(List.class::cast)
                    .orElse(Collections.emptyList());
        }

        @Override
        public long getDeviceConnectionTotal() {
            return getAllTransport()
                    .parallelStream()
                    .mapToLong(this::getDeviceConnectionTotal)
                    .sum();
        }

        @Override
        public long getDeviceConnectionTotal(Transport transport) {

            return Optional.ofNullable(cacheMap.get("transport-total:".concat(transport.name())))
                    .map(Number.class::cast)
                    .map(Number::longValue)
                    .orElse(0L);
        }

        @Override
        public Map<Transport, Long> getDeviceConnectionTotalGroup() {
            return getAllTransport()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), this::getDeviceConnectionTotal));
        }
    }

    private class CurrentLettuceGatewayServerInfo extends LettuceGatewayServerInfo {

        private Map<Transport, AtomicLong> counter = new ConcurrentHashMap<>();

        private Map<Transport, Set<String>> transports = new ConcurrentHashMap<>();


        public CurrentLettuceGatewayServerInfo(String id) {
            super(id);
            this.cacheMap.clear();
        }

        @SneakyThrows
        void reportDeviceCount(Transport transport, long count) {
            counter.computeIfAbsent(transport, __ -> new AtomicLong()).set(count);
            transports.computeIfAbsent(transport, __ -> new HashSet<>());

            this.cacheMap.put("transport-total:".concat(transport.name()), count);
            this.cacheMap.put("transports", new ArrayList<>(transports.keySet()));
        }

        @SneakyThrows
        void registerTransport(Transport transport, String... hosts) {
            transports.computeIfAbsent(transport, __ -> new HashSet<>()).addAll(Arrays.asList(hosts));
            this.cacheMap.put("transports", new ArrayList<>(transports.keySet()));
            this.cacheMap.put("transport-host:".concat(transport.name()), new ArrayList<>(transports.get(transport)));
        }

        @Override
        public long getDeviceConnectionTotal(Transport transport) {
            return counter.computeIfAbsent(transport, __ -> new AtomicLong()).get();
        }
    }

    protected LettuceGatewayServerInfo createGatewayServerInfo(String serverId) {
        return new LettuceGatewayServerInfo(serverId);
    }

    @Override
    public Optional<GatewayServerInfo> getServerInfo(String serverId) {
        LettuceGatewayServerInfo serverInfo= createGatewayServerInfo(serverId);
        if(!serverInfo.isAlive()){
            return Optional.empty();
        }
        return Optional.of(serverInfo);
    }

    @Override
    public GatewayServerInfo getCurrentServerInfo() {
        return current;
    }

    @Override
    @SneakyThrows
    public List<GatewayServerInfo> getAllServerInfo() {

        return plus
                .<String, String>getRedisAsync(StringCodec.getInstance())
                .thenCompose(redis -> redis.smembers("d_g_s_m:all"))
                .thenApply(all -> all.stream().map(this::createGatewayServerInfo).map(GatewayServerInfo.class::cast).collect(Collectors.toList()))
                .toCompletableFuture()
                .get();
    }

    @Override
    public void serverOffline(String serverId) {
        plus.<String, String>getRedisAsync(StringCodec.getInstance())
                .thenAccept(redis -> {
                    redis.srem("d_g_s_m:all", serverId);
                    redis.del("d_g_s_m:info:".concat(serverId));
                });
    }

    @Override
    public void registerTransport(Transport transport, String... hosts) {
        current.registerTransport(transport, hosts);
    }

    @Override
    public void reportDeviceCount(Transport transport, long count) {
        current.reportDeviceCount(transport, count);
    }

    @Override
    public long getDeviceCount(String serverId) {
        return getServerInfo(serverId).
                map(GatewayServerInfo::getDeviceConnectionTotal)
                .orElse(0L);
    }

    @Override
    public void onServerDown(Consumer<String> listener) {
        this.haManager.onNodeLeave(node -> listener.accept(node.getId()));
    }

    @Override
    public long getDeviceCount() {

        return getAllServerInfo()
                .parallelStream()
                .mapToLong(GatewayServerInfo::getDeviceConnectionTotal)
                .sum();
    }
}
