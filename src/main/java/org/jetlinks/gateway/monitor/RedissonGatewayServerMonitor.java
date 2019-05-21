package org.jetlinks.gateway.monitor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.protocol.message.codec.Transport;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private static final String transport_hosts = "transport_hosts";
    private static final String transport_all_support = "transport_supports";
    private static final String transport_connection_total = "transport_conn_total";


    public String getRedisKey(String... key) {
        return "device-gateway:" + String.join(":", key);
    }


    public RedissonGatewayServerMonitor(String currentServerId, RedissonClient redissonClient, ScheduledExecutorService executorService) {
        this.allServerId = redissonClient.getMap(getRedisKey("server:all"));
        this.client = redissonClient;
        this.currentServerId = currentServerId;
        this.executorService = executorService;
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
        return newGatewayServerInfo(currentServerId);
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
        log.debug("gateway server [{}] offline ", serverId);
        allServerId.fastRemove(serverId);

        client.<Transport>getSet(getRedisKey(transport_all_support, serverId))
                .forEach(transport -> {
                    client.getSet(getRedisKey(transport_connection_total, serverId, transport.name())).delete();
                    client.getSet(getRedisKey(transport_hosts, serverId, transport.name())).delete();
                });

        client.getSet(getRedisKey(transport_all_support, serverId))
                .delete();
    }

    @Override
    public void registerTransport(Transport transport, String... hosts) {
        client.getSet(getRedisKey(transport_all_support, currentServerId)).add(transport);
        client.getSet(getRedisKey(transport_hosts, currentServerId, transport.name())).addAll(Arrays.asList(hosts));
    }

    @Override
    public void reportDeviceCount(Transport transport, long count) {
        client.getAtomicLong(getRedisKey(transport_connection_total, currentServerId, transport.name())).set(count);
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

    @PreDestroy
    public void shutdown() {
        allServerId.remove(currentServerId);
    }

    @PostConstruct
    public void startup() {
        allServerId.put(currentServerId, System.currentTimeMillis());

        executorService.scheduleAtFixedRate(() -> {
            log.debug("gateway server [{}] keepalive", currentServerId);
            allServerId.put(currentServerId, System.currentTimeMillis());

            allServerId.entrySet()
                    .stream()
                    .filter(e -> System.currentTimeMillis() - e.getValue() > timeToLive * 1000)
                    .map(Map.Entry::getKey)
                    .filter(Objects::nonNull)
                    .forEach(this::serverOffline);

        }, 10, Math.max(1, timeToLive - 1), TimeUnit.SECONDS);


    }
}
