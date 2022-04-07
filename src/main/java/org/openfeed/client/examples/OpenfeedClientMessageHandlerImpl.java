package org.openfeed.client.examples;

import com.google.protobuf.InvalidProtocolBufferException;
import org.openfeed.OpenfeedGatewayMessage;
import org.openfeed.client.api.OpenfeedClientMessageHandler;
import org.openfeed.client.api.impl.PbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenfeedClientMessageHandlerImpl implements OpenfeedClientMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientMessageHandlerImpl.class);

    @Override
    public void onMessage(byte[] bytes) {
        log.info("{} bytes received",bytes.length);
        // How to decode
        try {
            OpenfeedGatewayMessage ofgm = OpenfeedGatewayMessage.parseFrom(bytes);
            log.info("Message: {}", PbUtil.toJson(ofgm));
        } catch (InvalidProtocolBufferException e) {
            log.error("Decode failed: {}",e.getMessage());
        }
    }
}
