package org.jetlinks.gateway.monitor;

import lombok.SneakyThrows;
import org.jetlinks.gateway.vertx.mqtt.RedissonHelper;
import org.jetlinks.protocol.message.codec.Transport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class RedissonGatewayServerMonitorTest {

    RedissonGatewayServerMonitor monitor;

    @Before
    public void init() {
        monitor = new RedissonGatewayServerMonitor("test", RedissonHelper.newRedissonClient(), Executors.newScheduledThreadPool(4));
        monitor.startup();
    }

    @After
    public void after() {
        monitor.shutdown();
    }

    @Test
    public void testOnlineOffline() {
        GatewayServerInfo info = monitor.getServerInfo("test").orElse(null);
        Assert.assertNotNull(info);

        Assert.assertEquals(info.getId(), "test");
        monitor.serverOffline("test");
        info = monitor.getServerInfo("test").orElse(null);
        Assert.assertNull(info);
    }

    @Test
    @SneakyThrows
    public void testInfo() {
        monitor.registerTransport( Transport.MQTT, "tcp://127.0.0.1:1883");
        monitor.registerTransport(Transport.TCP, "tcp://127.0.0.1:2190");

        monitor.reportDeviceCount(Transport.MQTT, 100);
        monitor.reportDeviceCount( Transport.TCP, 200);
        Thread.sleep(3000);

        GatewayServerInfo info = monitor.getServerInfo("test").orElse(null);
        Assert.assertNotNull(info);
        Assert.assertFalse(monitor.getAllServerInfo().isEmpty());

        Assert.assertTrue(info.getAllTransport().contains(Transport.MQTT));
        Assert.assertTrue(info.getAllTransport().contains(Transport.TCP));


        Assert.assertEquals(info.getDeviceConnectionTotal(Transport.MQTT), 100);
        Assert.assertEquals(info.getDeviceConnectionTotal(), 300);
        Assert.assertEquals(monitor.getDeviceCount(), 300);
        Assert.assertEquals(monitor.getDeviceCount("test"), 300);


        Assert.assertTrue(info.getAllTransport().contains(Transport.MQTT));

        Assert.assertTrue(info.getTransportHosts(Transport.MQTT).contains("tcp://127.0.0.1:1883"));


        monitor.serverOffline("test");
        Assert.assertTrue(info.getAllTransport().isEmpty());
        Assert.assertTrue(info.getTransportHosts(Transport.MQTT).isEmpty());
        Assert.assertEquals(info.getDeviceConnectionTotal(Transport.MQTT), 0);
        Assert.assertEquals(info.getDeviceConnectionTotal(), 0);

    }

}