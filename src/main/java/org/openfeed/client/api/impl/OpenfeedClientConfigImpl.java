package org.openfeed.client.api.impl;

import com.google.common.base.MoreObjects;
import org.openfeed.BulkSubscriptionFilter;
import org.openfeed.InstrumentDefinition;
import org.openfeed.Service;
import org.openfeed.SubscriptionType;
import org.openfeed.client.api.OpenfeedClientConfig;

import java.util.*;

public class OpenfeedClientConfigImpl implements OpenfeedClientConfig {
    private static final long RECONNECT_TIMEOUT_WAIT_MS = 2000;
    private static final int RCV_BUF_SIZE = 10 * (1024 * 1024);
    private static final int MAX_FRAME_PAYLOAD_SIZE = 128 * 1024;
    private static final int PROTOCOL_VERSION = 1;

    private String clientId = UUID.randomUUID().toString();
    // Connection
    private String scheme = "ws";
    private String host = "openfeed.aws.barchart.com";
    private int port = 80;
    private int protocolVersion = PROTOCOL_VERSION;
    private WireProtocol wireProtocol = WireProtocol.PB;
    private String userName = "";
    private String password = "";
    private String jwt = null;
    private boolean reconnect = true;
    private long reconnectDelayMs = RECONNECT_TIMEOUT_WAIT_MS;
    private int receiveBufferSize = RCV_BUF_SIZE;
    private int maxFramePayloadSize = MAX_FRAME_PAYLOAD_SIZE;

    // Subscriptions
    private String[] symbols = null;
    private long[] marketIds;
    private String[] exchanges;
    private int[] channelIds;
    //
    private Service service = Service.REAL_TIME;
    //
    private Set<SubscriptionType> subscriptionTypes = new HashSet<>();
    private Set<InstrumentDefinition.InstrumentType> instrumentTypes = new HashSet<>();
    private List<BulkSubscriptionFilter> bulkSubscriptionFilters = new ArrayList<>();
    private String [] spreadTypes;
    private int snapshotIntervalSec;
    private boolean instrumentRequest;
    private boolean instrumentCrossReferenceRequest;
    private boolean exchangeRequest;
    private int randomInstruments;
    // logging
    private boolean logRequestResponse;
    private boolean logAll;
    private boolean logHeartBeat;
    private boolean logInstrument;
    private boolean logSnapshot;
    private boolean logUpdates;
    private boolean logBbo;
    private boolean logTrade;
    private boolean logDepth;
    private boolean logTradeCancel;
    private boolean logTradeCorrection;
    private boolean logOhlc;
    private boolean logVolumeAtPrice;
    private boolean logPrettyPrint;
    private Set<String> logSymbols;
    private boolean logWire;
    //
    private int numberOfConnections = 1;
    private int statsDisplaySeconds = 30;
    private boolean wireStats = false;
    private int wireStatsDisplaySeconds = 0;
    private boolean disableClientOnDuplicateLogin = true;

    public OpenfeedClientConfigImpl dup() throws CloneNotSupportedException {
        OpenfeedClientConfigImpl o = new OpenfeedClientConfigImpl();
        o.scheme = this.scheme;
        o.host = this.host;
        o.port = this.port;
        o.protocolVersion = this.protocolVersion;
        o.wireProtocol = this.wireProtocol;
        o.userName = this.userName;
        o.password = this.password;
        o.jwt = this.jwt;
        o.reconnect = this.reconnect;
        o.reconnectDelayMs = this.reconnectDelayMs;
        o.receiveBufferSize = this.receiveBufferSize;
        o.maxFramePayloadSize = this.maxFramePayloadSize;
        //
        o.symbols = this.symbols;
        o.marketIds = this.marketIds;
        o.exchanges = this.exchanges;
        o.channelIds = this.channelIds;
        //
        o.subscriptionTypes.addAll(this.subscriptionTypes);
        o.instrumentTypes.addAll(this.instrumentTypes);
        o.bulkSubscriptionFilters.addAll(this.bulkSubscriptionFilters);
        o.spreadTypes = this.spreadTypes;
        //
        o.snapshotIntervalSec = this.snapshotIntervalSec;
        o.instrumentRequest = this.instrumentRequest;
        o.instrumentCrossReferenceRequest = this.instrumentCrossReferenceRequest;
        o.randomInstruments = this.randomInstruments;
        o.exchangeRequest = this.exchangeRequest;
        //
        o.randomInstruments = this.randomInstruments;
        o.logRequestResponse = this.logRequestResponse;
        o.logAll = this.logAll;
        o.logHeartBeat = this.logHeartBeat;
        o.logInstrument = this.logInstrument;
        o.logSnapshot = this.logSnapshot;
        o.logUpdates = this.logUpdates;
        o.logBbo = this.logBbo;
        o.logTrade = this.logTrade;
        o.logDepth = this.logDepth;
        o.logTradeCancel = this.logTradeCancel;
        o.logTradeCorrection = this.logTradeCorrection;
        o.logOhlc = this.logOhlc;
        o.logVolumeAtPrice = this.logVolumeAtPrice;
        o.logSymbols = this.logSymbols;
        o.logWire = this.logWire;
        //
        o.numberOfConnections = this.numberOfConnections;
        o.statsDisplaySeconds = this.statsDisplaySeconds;
        o.wireStats = this.wireStats;
        o.wireStatsDisplaySeconds = this.wireStatsDisplaySeconds;
        return o;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("server", scheme + "//" + host + ":" + port).add("userName", userName)
                .toString();
    }

    @Override
    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public int getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    @Override
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String getJwt() {
        return jwt;
    }

    public void setJwt(String jwt) {
        this.jwt = jwt;
    }

    @Override
    public String[] getSymbols() {
        return this.symbols;
    }

    public void setSymbols(String[] symbols) {
        this.symbols = symbols;
    }

    @Override
    public Service getService() {
        return this.service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public boolean isLogUpdate() {
        return this.logUpdates;
    }

    public void setLogUpdates(boolean b) {
        this.logUpdates = b;

    }

    @Override
    public WireProtocol getWireProtocol() {
        return this.wireProtocol;
    }

    public void setWireProtocol(WireProtocol wp) {
        this.wireProtocol = wp;
    }

    @Override
    public boolean isLogBbo() {
        return logBbo;
    }

    public void setLogBbo(boolean logBbo) {
        this.logBbo = logBbo;
    }

    @Override
    public boolean isLogTrade() {
        return logTrade;
    }

    @Override
    public boolean isLogDepth() {
        return this.logDepth;
    }

    @Override
    public boolean isLogTradeCancel() {
        return this.logTradeCancel;
    }

    @Override
    public boolean isLogTradeCorrection() {
        return this.logTradeCorrection;
    }

    @Override
    public boolean isLogOhlc() {
        return this.logOhlc;
    }

    public void setLogVolumeAtPrice(boolean logVolumeAtPrice) {
        this.logVolumeAtPrice = logVolumeAtPrice;
    }

    @Override
    public boolean isLogVolumeAtPrice() {
        return this.logVolumeAtPrice;
    }

    public void setLogOhlc(boolean logOhlc) {
        this.logOhlc = logOhlc;
    }

    public void setLogTrade(boolean logTrades) {
        this.logTrade = logTrades;
    }

    public void setLogDepth(boolean v) {
        this.logDepth = v;
    }

    @Override
    public boolean isLogInstrument() {
        return logInstrument;
    }

    @Override
    public boolean isExchangeRequest() {
        return exchangeRequest;
    }

    public void setExchangeRequest(boolean exchangeRequest) {
        this.exchangeRequest = exchangeRequest;
    }

    @Override
    public boolean isLogSnapshot() {
        return logSnapshot;
    }

    public boolean isLogUpdates() {
        return logUpdates;
    }

    public boolean isLogTrades() {
        return logTrade;
    }

    public void setLogInstrument(boolean logInstrument) {
        this.logInstrument = logInstrument;
    }

    public void setLogSnapshot(boolean logSnapshot) {
        this.logSnapshot = logSnapshot;
    }

    @Override
    public long[] getMarketIds() {
        return marketIds;
    }

    public void setMarketIds(long[] marketIds) {
        this.marketIds = marketIds;
    }

    @Override
    public String[] getExchanges() {
        return this.exchanges;
    }

    public void setExchanges(String[] exchanges) {
        this.exchanges = exchanges;
    }
    public int[] getChannelIds() {
        return channelIds;
    }

    public void setChannelIds(int[] channelIds) {
        this.channelIds = channelIds;
    }

    @Override
    public int getNumberOfConnections() {
        return this.numberOfConnections;
    }

    public void setNumberOfConnections(int numberOfConnections) {
        this.numberOfConnections = numberOfConnections;
    }

    @Override
    public SubscriptionType [] getSubscriptionTypes() {
        return subscriptionTypes.toArray(new SubscriptionType[0]);
    }

    public void addSubscriptonType(SubscriptionType type) {
        this.subscriptionTypes.add(type);
    }

    public void setSubTypes(String [] types) {
        if(types == null) {
            return;
        }
        for(String t : types) {
            SubscriptionType st = SubscriptionType.valueOf(t.toUpperCase());
            addSubscriptonType(st);
        }
    }

    @Override
    public InstrumentDefinition.InstrumentType[] getInstrumentTypes() {
        return this.instrumentTypes.toArray(new InstrumentDefinition.InstrumentType[0]);
    }

    @Override
    public String[] getSpreadTypes() {
        if(spreadTypes == null) {
            return new String[0];
        }
        return this.spreadTypes;
    }

    public void setSpreadTypes(String [] spreadTypes) {
        this.spreadTypes = spreadTypes;
    }

    public void addInstrumentType(InstrumentDefinition.InstrumentType type) {
        this.instrumentTypes.add(type);
    }

    public void setIntTypes(String [] types) {
        if(types == null) {
            return;
        }
        for(String t : types) {
            InstrumentDefinition.InstrumentType it = InstrumentDefinition.InstrumentType.valueOf(t.toUpperCase());
            addInstrumentType(it);
        }
    }

    public void setInstrumentRequest(boolean b) {
        this.instrumentRequest = b;
    }

    public void setInstrumentCrossReferenceRequest(boolean b) {
        this.instrumentCrossReferenceRequest = b;
    }


    @Override
    public boolean isInstrumentRequest() {
        return instrumentRequest;
    }

    @Override
    public boolean isInstrumentCrossReferenceRequest() {
        return instrumentCrossReferenceRequest;
    }

    @Override
    public boolean isLogRequestResponse() {
        return logRequestResponse;
    }

    public void setLogRequestResponse(boolean logRequestResponse) {
        this.logRequestResponse = logRequestResponse;
    }

    @Override
    public boolean isLogAll() {
        return this.logAll;
    }

    public void setLogAll(boolean logAll) {
        this.logAll = logAll;
    }

    public void setRandomInstruments(int numInstruments) {
        this.randomInstruments = numInstruments;
    }

    @Override
    public int getRandomInstruments() {
        return this.randomInstruments;
    }

    @Override
    public int getStatsDisplaySeconds() {
        return this.statsDisplaySeconds;
    }

    @Override
    public boolean isWireStats() {
        return this.wireStats;
    }
    public void setWireStats(boolean v) {
        this.wireStats = v;
    }

    @Override
    public int getWireStatsDisplaySeconds() {
        return wireStatsDisplaySeconds;
    }

    public void setWireStatsDisplaySeconds(int wireStatsDisplaySeconds) {
        this.wireStatsDisplaySeconds = wireStatsDisplaySeconds;
    }

    @Override
    public boolean isDisableClientOnDuplicateLogin() {
        return this.disableClientOnDuplicateLogin;
    }

    @Override
    public BulkSubscriptionFilter[] getBulkSubscriptionFilters() {
        return bulkSubscriptionFilters.toArray(new BulkSubscriptionFilter[0]);
    }

    public void addBulkSubscriptionFilter(BulkSubscriptionFilter bulkSubscriptionFilter) {
        this.bulkSubscriptionFilters.add(bulkSubscriptionFilter);
    }

    public void setDisableClientOnDuplicateLogin(boolean v) {
        this.disableClientOnDuplicateLogin = v;
    }



    public void setStatsDisplaySeconds(int sec) {
        this.statsDisplaySeconds = sec;
    }

    @Override
    public boolean isLogHeartBeat() {
        return this.logHeartBeat;
    }

    public void setLogHeartBeat(boolean logHeartBeat) {
        this.logHeartBeat = logHeartBeat;
    }

    @Override
    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    @Override
    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public long getReconnectDelayMs() {
        return this.reconnectDelayMs;
    }

   

    public void setReconnectDelayMs(long sec) {
        this.reconnectDelayMs = sec;
    }

    public void setLogTradeCancel(boolean v) {
        this.logTradeCancel = v;
    }

    public void setLogTradeCorrection(boolean v) {
        this.logTradeCorrection = v;
    }

    public Set<String> getLogSymbols() {
        return logSymbols;
    }

    public void setLogSymbols(Set<String> logSymbols) {
        this.logSymbols = logSymbols;
    }

    @Override
    public boolean isLogSymbol(String s) {
        return logSymbols != null ? logSymbols.contains(s) : false;
    }

    @Override
    public boolean isLogPrettyPrint() {
        return logPrettyPrint;
    }

    public void setLogPrettyPrint(boolean logPrettyPrint) {
        this.logPrettyPrint = logPrettyPrint;
    }

    @Override
    public boolean isLogWire() {
        return logWire;
    }

    public void setLogWire(boolean logWire) {
        this.logWire = logWire;
    }

    @Override
    public int getSnapshotIntervalSec() {
        return snapshotIntervalSec;
    }

    @Override
    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getMaxFramePayloadSize() {
        return maxFramePayloadSize;
    }

    public void setMaxFramePayloadSize(int maxFramePayloadSize) {
        this.maxFramePayloadSize = maxFramePayloadSize;
    }

    public void setSnapshotIntervalSec(int snapshotIntervalSec) {
        this.snapshotIntervalSec = snapshotIntervalSec;
    }
}
