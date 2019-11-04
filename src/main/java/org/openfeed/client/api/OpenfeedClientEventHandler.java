package org.openfeed.client.api;

public interface OpenfeedClientEventHandler {
    // Connection Related Events
    void onEvent(OpenfeedClient client, OpenfeedEvent event);
}
