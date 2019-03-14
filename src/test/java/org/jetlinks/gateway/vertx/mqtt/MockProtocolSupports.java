package org.jetlinks.gateway.vertx.mqtt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.Unpooled;
import org.jetlinks.protocol.ProtocolSupport;
import org.jetlinks.protocol.ProtocolSupports;
import org.jetlinks.protocol.message.CommonDeviceMessageReply;
import org.jetlinks.protocol.message.DeviceMessage;
import org.jetlinks.protocol.message.codec.*;
import org.jetlinks.protocol.message.function.FunctionInvokeMessage;
import org.jetlinks.protocol.message.property.ReadPropertyMessage;
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
                    public EncodedMessage encode(Transport transport, MessageEncodeContext context) {
                        if(transport==Transport.MQTT) {
                            if(context.getMessage() instanceof FunctionInvokeMessage){
                                FunctionInvokeMessage readPropertyMessage=(FunctionInvokeMessage)context.getMessage();

                                return EncodedMessage.mqtt(context.getMessage().getDeviceId(), "/key/func-invoke/"+readPropertyMessage.getFunctionId(),
                                        Unpooled.copiedBuffer(context.getMessage().toJson().toJSONString().getBytes()));
                            }
                            return EncodedMessage.mqtt(context.getMessage().getDeviceId(), "/key/xxxx/command",
                                    Unpooled.copiedBuffer(context.getMessage().toJson().toJSONString().getBytes()));
                        }
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeviceMessage decode(Transport transport, MessageDecodeContext context) {
                        JSONObject jsonObject = JSON.parseObject(context.getMessage().getByteBuf().toString(StandardCharsets.UTF_8));
                        //回复确认收到
                        if (context instanceof FromDeviceMessageContext) {
                            ((FromDeviceMessageContext) context).sendToDevice(
                                    EncodedMessage.mqtt(context.getMessage().getDeviceId(), "ack",
                                            Unpooled.copiedBuffer(context.getMessage().getByteBuf()))
                            );
                        }
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
