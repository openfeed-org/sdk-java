package org.openfeed.client.api;

import org.openfeed.BulkSubscriptionFilter;
import org.openfeed.InstrumentDefinition;
import org.openfeed.Service;
import org.openfeed.SubscriptionType;

public interface OpenfeedClientConfig {

    // Unique Id for Client
    String getClientId();

    // Connection
    String getScheme();
    String getHost();
    int getPort();
    int getProtocolVersion();
    WireProtocol getWireProtocol();
	String getUserName();
	String getPassword();
    // JSON Web Token
    String getJwt();
    // Get sessionId, which passed to the server as a query parameter
    String getParameterSessionId();
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
    String [] getSpreadTypes();
    int getSnapshotIntervalSec();
    boolean isInstrumentRequest();
    boolean isInstrumentCrossReferenceRequest();
    boolean isExchangeRequest();
    boolean isListSubscriptionsRequest();
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
    boolean isLogVolumeAtPrice();
    boolean isLogPrettyPrint();
    boolean isLogSymbol(String symbol);
    boolean isLogWire();

    int getNumberOfConnections();
    int getStatsDisplaySeconds();
    int getWireStatsDisplaySeconds();
    // If > 0 will send a websocket ping to the server every X seconds
    int getPingSeconds();

    boolean isDisableClientOnDuplicateLogin();

    BulkSubscriptionFilter[] getBulkSubscriptionFilters();

    enum WireProtocol {
        PB, JSON
    }

    String PARAMETER_SESSION_ID = "sessionId";

}
