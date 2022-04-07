package org.openfeed.client.api;

/**
 *  Callback for every message received.
 */
public interface OpenfeedClientMessageHandler {

    /**
     * Will be called for every message.
     *
     * @param bytes A copy of the Protobuf encoded OpenfeedGatewayMessage
     */
    void onMessage(byte [] bytes);

}
