package org.jetlinks.gateway.vertx.mqtt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.Unpooled;
import org.jetlinks.protocol.ProtocolSupport;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.message.CommonDeviceMessageReply;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.codec.DeviceMessageCodec;
import org.jetlinks.protocol.message.codec.EncodedMessage;
import org.jetlinks.protocol.message.codec.Transport;
import org.jetlinks.protocol.message.property.ReadPropertyMessageReply;
import org.jetlinks.protocol.metadata.DeviceMetadataCodec;

import java.nio.charset.StandardCharsets;

/**
 * @author zhouhao
 * @since 1.0.0
 */
public class MockProtocolSupports implements ProtocolSupports {
    @Override
    public ProtocolSupport getProtocol(String protocol) {
        return new ProtocolSupport() {
            @Override
            public String getId() {
                return "mock";
            }

            @Override
            public String getName() {
                return "模拟协议";
            }

            @Override
            public String getDescription() {
                return "";
            }

            @Override
            public DeviceMessageCodec getMessageCodec() {

                return new DeviceMessageCodec() {
                    @Override
                    public EncodedMessage encode(Transport transport, DeviceMessage deviceMessage) {
                        return EncodedMessage.mqtt(deviceMessage.getDeviceId(), "command",
                                Unpooled.copiedBuffer(deviceMessage.toJson().toJSONString().getBytes()));
                    }

                    @Override
                    public DeviceMessage decode(Transport transport, EncodedMessage message) {
                        JSONObject jsonObject = JSON.parseObject(message.getByteBuf().toString(StandardCharsets.UTF_8));
                        if ("read-property".equals(jsonObject.get("type"))) {
                            return jsonObject.toJavaObject(ReadPropertyMessageReply.class);
                        }
                        return jsonObject.toJavaObject(CommonDeviceMessageReply.class);
                    }
                };
            }

            @Override
            public DeviceMetadataCodec getMetadataCodec() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
