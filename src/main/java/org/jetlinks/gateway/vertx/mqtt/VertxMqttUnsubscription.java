package org.jetlinks.gateway.vertx.mqtt;

import io.vertx.mqtt.messages.MqttUnsubscribeMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.server.mqtt.MqttUnsubscription;

import java.util.List;

@Slf4j
public class VertxMqttUnsubscription implements MqttUnsubscription {

    @Getter
    private MqttDeviceSession session;

    private MqttUnsubscribeMessage message;

    private volatile boolean accepted = false;

    public VertxMqttUnsubscription(MqttDeviceSession session, MqttUnsubscribeMessage message) {
        this.message = message;
        this.session = session;
    }

    @Override
    public int getMessageId() {
        return message.messageId();
    }

    @Override
    public List<String> getTopics() {
        return message.topics();
    }

    @Override
    public synchronized void accept() {
        if (accepted) {
            return;
        }
        accepted = true;
        log.debug("UNSUBSCRIBE: [{}]:{}", session.getDeviceId(), message.topics());
        session.getEndpoint()
                .unsubscribeAcknowledge(message.messageId());
    }
}
