package org.jetlinks.gateway.vertx.mqtt;

import io.vertx.mqtt.MqttEndpoint;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.server.mqtt.AckType;
import org.jetlinks.core.server.mqtt.MqttClientAck;

@Slf4j
public class VertxMqttClientAck implements MqttClientAck {

    private MqttDeviceSession session;

    private AckType ackType;

    private int messageId;

    public VertxMqttClientAck(MqttDeviceSession session, AckType ackType, int messageId) {
        this.session = session;
        this.ackType = ackType;
        this.messageId = messageId;
    }

    @Getter
    private volatile boolean acked;

    @Override
    public MqttDeviceSession getSession() {
        return session;
    }

    @Override
    public int getMessageId() {
        return messageId;
    }

    @Override
    public AckType getAckType() {
        return ackType;
    }

    @Override
    public synchronized void doAck() {
        if (acked) {
            return;
        }
        if (log.isInfoEnabled()) {
            log.info("do ack [{}] client[{}] message[{}]", ackType, session.getDeviceId(), messageId);
        }

        MqttEndpoint endpoint = session.getEndpoint();
        switch (ackType) {
            case PUBACK:
                endpoint.publishAcknowledge(messageId);
                return;
            case PUBREL:
                endpoint.publishRelease(messageId);
                return;
            case PUBCOMP:
                endpoint.publishComplete(messageId);
                return;
            case PUBREC:
                endpoint.publishReceived(messageId);
                return;
        }
        acked = true;

    }
}
