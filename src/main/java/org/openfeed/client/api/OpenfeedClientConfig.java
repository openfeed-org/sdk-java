package org.openfeed.client.api;

import org.openfeed.InstrumentDefinition;
import org.openfeed.Service;
import org.openfeed.SubscriptionType;

import java.util.List;

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
    long getReconnectDelayMs();
    int getReceiveBufferSize();

    // Service Type
    Service getService();

    // Subscriptions/Requests
	String[] getSymbols();
    long[] getMarketIds();
    String[] getExchanges();
    int[] getChannelIds();

    SubscriptionType [] getSubscriptionTypes();
    InstrumentDefinition.InstrumentType [] getInstrumentTypes();
    int getSnapshotIntervalSec();
    boolean isInstrumentRequest();
    boolean isInstrumentCrossReferenceRequest();
    boolean isExchangeRequest();
    int getRandomInstruments();

    // Logging
    boolean isLogRequestResponse();
    boolean isLogAll();
    boolean isLogHeartBeat();
    boolean isLogInstrument();
    boolean isLogSnapshot();
    boolean isLogUpdate();
    boolean isLogBbo();
    boolean isLogTrade();
    boolean isLogDepth();
    boolean isLogTradeCancel();
    boolean isLogTradeCorrection();
    boolean isLogOhlc();
    boolean isLogPrettyPrint();
    boolean isLogSymbol(String symbol);

    int getNumberOfConnections();
    int getStatsDisplaySeconds();
    boolean isWireStats();
    boolean isDisableClientOnDuplicateLogin();



    enum WireProtocol {
        PB, JSON
    }
}
