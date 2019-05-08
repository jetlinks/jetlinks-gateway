package org.jetlinks.gateway.vertx.tcp;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.protocol.device.AuthenticationResponse;

@Getter
@Setter
public class TcpAuthenticationResponse extends AuthenticationResponse {

    private String deviceId;

    public static TcpAuthenticationResponse success(String deviceId) {
        TcpAuthenticationResponse response = new TcpAuthenticationResponse();
        response.setSuccess(true);
        response.setCode(200);
        response.setMessage("授权通过");
        return response;
    }

    public static TcpAuthenticationResponse error(int code, String message) {
        TcpAuthenticationResponse response = new TcpAuthenticationResponse();
        response.setSuccess(false);
        response.setCode(code);
        response.setMessage(message);
        return response;
    }
}
