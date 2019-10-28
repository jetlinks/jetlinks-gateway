package org.jetlinks.gateway.vertx.mqtt;

import io.netty.buffer.ByteBuf;
import io.vertx.mqtt.messages.MqttPublishMessage;
import lombok.AllArgsConstructor;
import org.jetlinks.core.message.codec.MqttMessage;

import javax.annotation.Nonnull;

@AllArgsConstructor
public class VertxMqttMessage implements MqttMessage {

    private String deviceId;

    private MqttPublishMessage message;

    @Override
    public int getMessageId() {
        return message.messageId();
    }

    @Nonnull
    @Override
    public String getTopic() {
        return message.topicName();
    }

    @Override
    public int getQosLevel() {
        return message.qosLevel().value();
    }

    @Override
    public boolean isDup() {
        return message.isDup();
    }

    @Override
    public boolean isRetain() {
        return message.isRetain();
    }

    @Nonnull
    @Override
    public ByteBuf getPayload() {
        return message.payload().getByteBuf();
    }

    @Nonnull
    @Override
    public String getDeviceId() {
        return deviceId;
    }

    @Override
    public String toString() {
        return deviceId + " => " + getTopic() + " | messageId " + getMessageId() + " | QoS " + getQosLevel() + " | dup " + isDup() + " | retain " + isRetain() + " | will " + isWill();
    }
}
