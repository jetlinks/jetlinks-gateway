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
import org.jetlinks.protocol.message.property.ReadPropertyMessageReply;
import org.jetlinks.protocol.metadata.DeviceMetadataParser;

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
            public DeviceMessageCodec getMessageConverter() {

                return new DeviceMessageCodec() {
                    @Override
                    public EncodedMessage convert(DeviceMessage deviceMessage) {
                        return EncodedMessage.mqtt(deviceMessage.getDeviceId(), "command",
                                Unpooled.copiedBuffer(deviceMessage.toJson().toJSONString().getBytes()));
                    }

                    @Override
                    public DeviceMessage convert(EncodedMessage message) {
                        JSONObject jsonObject = JSON.parseObject(message.getByteBuf().toString(StandardCharsets.UTF_8));
                        if ("read-property".equals(jsonObject.get("type"))) {
                            return jsonObject.toJavaObject(ReadPropertyMessageReply.class);
                        }
                        return jsonObject.toJavaObject(CommonDeviceMessageReply.class);
                    }
                };
            }

            @Override
            public DeviceMetadataParser getMetadataParser() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
