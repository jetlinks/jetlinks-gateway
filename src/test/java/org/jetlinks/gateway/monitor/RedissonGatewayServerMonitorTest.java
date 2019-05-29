package org.jetlinks.gateway.monitor;

import lombok.SneakyThrows;
import org.jetlinks.gateway.vertx.mqtt.RedissonHelper;
import org.jetlinks.core.message.codec.Transport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;

public class RedissonGatewayServerMonitorTest {

    RedissonGatewayServerMonitor monitor;

    @Before
    public void init() {
        monitor = new RedissonGatewayServerMonitor("test", RedissonHelper.newRedissonClient(), Executors.newScheduledThreadPool(4));
        monitor.setTimeToLive(2);
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
        monitor.registerTransport(Transport.MQTT, "tcp://127.0.0.1:1883");
        monitor.registerTransport(Transport.TCP, "tcp://127.0.0.1:2190");

        monitor.reportDeviceCount(Transport.MQTT, 100);
        monitor.reportDeviceCount(Transport.TCP, 200);
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

        //主动下线,可能由于网络波动,导致心跳失败.
        monitor.serverOffline("test");
        Assert.assertFalse(monitor.getServerInfo("test").isPresent());

        Assert.assertTrue(monitor.getAllServerInfo().isEmpty());
        //暂停3秒,等待重新心跳
        Thread.sleep(3000);
        //重新上线
        Assert.assertTrue(monitor.getServerInfo("test").isPresent());
        Assert.assertTrue(info.getAllTransport().contains(Transport.MQTT));
        Assert.assertTrue(info.getTransportHosts(Transport.MQTT).contains("tcp://127.0.0.1:1883"));


    }

}