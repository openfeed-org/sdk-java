package org.openfeed.client.api;

import org.openfeed.SubscriptionType;

public interface OpenfeedClientConfig {

    // Unique Id for Client
    String getClientId();

    // Connection
    String getHost();
    int getPort();
    WireProtocol getWireProtocol();
	String getUserName();
	String getPassword();
    // Re-connects if the connection is dropped. Defaults to true.
    boolean isReconnect();
    long getReconnectDelaySec();

    // Subscriptions
	String[] getSymbols();
    long[] getMarketIds();
    String[] getExchanges();
    int[] getChannelIds();

    // Types
    SubscriptionType getSubcriptionType();
    boolean isInstrumentRequest();
    boolean isInstrumentCrossReferenceRequest();
    int getRandomInstruments();

    // Logging
    boolean isLogAll();
    boolean isLogHeartBeat();
    boolean isLogInstrument();
    boolean isLogSnapshot();
    boolean isLogUpdate();
    boolean isLogBbo();
    boolean isLogTrade();
    boolean isLogTradeCancel();
    boolean isLogTradeCorrection();

    int getNumberOfConnections();
    int getStatsDisplaySeconds();

    enum WireProtocol {
        PB, JSON
    }

}
