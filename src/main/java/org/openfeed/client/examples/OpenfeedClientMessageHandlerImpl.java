package org.openfeed.client.examples;

import org.openfeed.OpenfeedGatewayMessage;
import org.openfeed.client.api.OpenfeedClientMessageHandler;
import org.openfeed.client.api.impl.PbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenfeedClientMessageHandlerImpl implements OpenfeedClientMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientMessageHandlerImpl.class);

    @Override
    public void onMessage(OpenfeedGatewayMessage message, byte[] bytes) {
        log.info("bytesReceived: {} message: {}",bytes.length, PbUtil.toJson(message));
    }
}
