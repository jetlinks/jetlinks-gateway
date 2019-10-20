package org.jetlinks.gateway.vertx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.messages.MqttSubscribeMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetlinks.core.server.mqtt.MqttSubscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class VertxMqttSubscription implements MqttSubscription {

    @Getter
    private MqttDeviceSession session;

    private MqttSubscribeMessage message;

    @Override
    public int getMessageId() {
        return message.messageId();
    }

    @Override
    public List<Topic> getTopics() {
        return message
                .topicSubscriptions()
                .stream()
                .map(VertxTopic::new)
                .collect(Collectors.toList());
    }

    @Override
    public void accept(Integer... qos) {
        List<MqttQoS> qosList = new ArrayList<>();
        List<Integer> accept = Arrays.asList(qos);
        for (MqttTopicSubscription topicSubscription : message.topicSubscriptions()) {
            if (qos.length == 0 || accept.contains(topicSubscription.qualityOfService().value())) {
                qosList.add(topicSubscription.qualityOfService());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("mqtt subscribe acknowledge:{} qos:{}", this, qosList);
        }
        session.getEndpoint().subscribeAcknowledge(getMessageId(), qosList);
    }

    @Override
    public String toString() {
        return "topics:" + getTopics();
    }

    @AllArgsConstructor
    class VertxTopic implements Topic {
        private MqttTopicSubscription subscription;

        @Override
        public String getName() {
            return subscription.topicName();
        }

        @Override
        public int getQos() {
            return subscription.qualityOfService().value();
        }

        @Override
        public String toString() {
            return getName() + "(QoS:" + subscription.qualityOfService() + ")";
        }
    }


}
