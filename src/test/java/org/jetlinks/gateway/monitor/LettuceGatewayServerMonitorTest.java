package org.jetlinks.gateway.monitor;

import lombok.SneakyThrows;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.lettuce.supports.DefaultLettucePlus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LettuceGatewayServerMonitorTest {

    LettuceGatewayServerMonitor monitor;

    @Before
    public void init() {
        monitor = new LettuceGatewayServerMonitor("test", DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient()));
        monitor.startup();
    }

    @After
    public void shutdown(){
        monitor.shutdown();;
    }

    @Test
    @SneakyThrows
    public void testOnlineOffline() {
        LettuceGatewayServerMonitor monitor2=  new LettuceGatewayServerMonitor("test2", DefaultLettucePlus.standalone(RedisClientHelper.createRedisClient()));
        monitor2.startup();

        monitor2.registerTransport(Transport.MQTT,"tcp://127.0.0.1:1883");
        monitor2.reportDeviceCount(Transport.MQTT,100);
        Thread.sleep(200);
        Assert.assertTrue(monitor.getServerInfo("test2")
                .map(info->info.getTransportHosts(Transport.MQTT))
                .orElse(Collections.emptyList())
                .contains("tcp://127.0.0.1:1883"));

        Assert.assertEquals((Object) 100L,monitor.getServerInfo("test2").map(GatewayServerInfo::getDeviceConnectionTotal).orElse(0L));

        CountDownLatch latch=new CountDownLatch(1);
        monitor.onServerDown(monitor-> latch.countDown());
        monitor2.shutdown();
        latch.await(10, TimeUnit.SECONDS);

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




    }

}