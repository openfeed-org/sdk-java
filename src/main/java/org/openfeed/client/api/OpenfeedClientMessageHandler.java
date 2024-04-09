package org.openfeed.client.api;

import org.openfeed.OpenfeedGatewayMessage;

/**
 *  Callback for every message received.
 */
public interface OpenfeedClientMessageHandler {

    /**
     * Will be called for every message.
     * @param message Decoded Openfeed Message
     * @param bytes A copy of the Protobuf encoded OpenfeedGatewayMessage
     */
    void onMessage(OpenfeedGatewayMessage message, byte [] bytes);

}
