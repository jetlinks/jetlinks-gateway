package org.jetlinks.gateway.vertx.tcp.message;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

@Getter
@AllArgsConstructor
public enum MessageType {
    PING((byte) 1),
    AUTH((byte) 2),
    MESSAGE((byte) 3);

    public final byte type;

   public static Optional<MessageType> of(byte type) {
        for (MessageType value : values()) {
            if (value.type == type) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}