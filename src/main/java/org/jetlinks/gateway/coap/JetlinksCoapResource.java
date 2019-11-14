package org.jetlinks.gateway.coap;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP;
import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.core.server.resources.Resource;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.device.DeviceRegistry;
import org.jetlinks.core.message.codec.CoapMessage;
import org.jetlinks.core.message.codec.EncodedMessage;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.message.codec.Transport;
import org.jetlinks.supports.server.ClientMessageHandler;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Slf4j
public class JetlinksCoapResource extends CoapResource {


    public JetlinksCoapResource() {
        super("jetlinks-coap", true);
    }

    @Setter
    private DeviceRegistry registry;

    @Setter
    private int clientOptionNumber = 2100;

    @Getter
    private Transport transport;

    @Setter
    @Getter
    private ClientMessageHandler messageHandler;

    private Optional<CoapDeviceRequest> getDeviceInfo(CoapExchange exchange) {

        String clientId = null;

        for (Option other : exchange.getRequestOptions().getOthers()) {
            if (other.getNumber() == clientOptionNumber) {
                clientId = other.getStringValue();
            }
        }

        if (clientId == null) {
            return Optional.empty();
        }

        return Optional.of(new CoapDeviceRequest(clientId));
    }

    @AllArgsConstructor
    @Getter
    private class CoapDeviceRequest {
        private String deviceId;
    }

    @Override
    public Resource getChild(String name) {
        return this;
    }

    @Override
    public void handlePOST(CoapExchange exchange) {
        Mono.justOrEmpty(getDeviceInfo(exchange))
                .map(CoapDeviceRequest::getDeviceId)
                .flatMap(registry::getDevice)
                .flatMap(device -> messageHandler
                        .handleMessage(device, getTransport(), new MessageDecodeContext() {
                            @Override
                            public EncodedMessage getMessage() {
                                return new CoapMessage(device.getDeviceId(), exchange);
                            }

                            @Override
                            public DeviceOperator getDevice() {
                                return device;
                            }
                        }))
                .switchIfEmpty(Mono.fromRunnable(() -> {
                    log.warn("unknown coap request:{}", exchange);
                    exchange.respond(CoAP.ResponseCode.BAD_REQUEST);
                    exchange.reject();
                }))
                .subscribe();
    }
}
