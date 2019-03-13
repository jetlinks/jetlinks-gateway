package org.jetlinks.gateway.session;

import lombok.Getter;
import lombok.Setter;

/**
 * @author zhouhao
 * @since 1.0.0
 */
@Getter
@Setter
public class DeviceSession {
    private String sessionId;

    private String creatorDeviceId;
}
