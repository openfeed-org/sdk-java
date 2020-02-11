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

    // Subscriptions/Requests
	String[] getSymbols();
    long[] getMarketIds();
    String[] getExchanges();
    int[] getChannelIds();

    // Types
    SubscriptionType getSubcriptionType();
    boolean isInstrumentRequest();
    boolean isInstrumentCrossReferenceRequest();
    boolean isExchangeRequest();
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
    boolean isLogPrettyPrint();
    boolean isLogSymbol(String symbol);

    int getNumberOfConnections();
    int getStatsDisplaySeconds();



    enum WireProtocol {
        PB, JSON
    }

}
