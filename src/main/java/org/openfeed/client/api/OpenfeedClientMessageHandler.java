package org.openfeed.client.api;

import org.openfeed.OpenfeedGatewayMessage;

/**
 *   Callback for every message received.
 */
public interface OpenfeedClientMessageHandler {

    void onMessage(OpenfeedGatewayMessage message);

}
