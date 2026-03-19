package org.openfeed.client.api.impl;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.Status;
import org.openfeed.SubscriptionRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Subscription {

    public enum SubscriptionState {
        Pending, Subscribed, UnSubscribed
    }
    private final String subscriptionId;
    private long correlationId;
    private SubscriptionRequest request;
    private  boolean exchangeSubscription;
    private String[] symbols;
    private Long[] marketIds;
    private String[] exchanges;
    private Integer [] channelIds;
    private final Map<String, SubscriptionState> symboltoState = new HashMap<>();
    private final Map<Long, SubscriptionState> marketIdtoState = new Long2ObjectHashMap<SubscriptionState>();
    private final Map<String, SubscriptionState> exchangetoState = new HashMap<>();
    private final Map<Integer, SubscriptionState> channelIdtoState = new Int2ObjectHashMap<SubscriptionState>();
    // SubscriptionResponse
    private Status status;

    public Subscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols,
            long correlationId, boolean exchangeSubscription) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.correlationId  = correlationId;
        this.exchangeSubscription = exchangeSubscription;
        if (!exchangeSubscription) {
            this.symbols = symbols;
            for (String symbol : this.symbols) {
                symboltoState.put(symbol, SubscriptionState.Pending);
            }
        } else {
            this.exchanges = symbols;
            for (String exchange : exchanges) {
                exchangetoState.put(exchange, SubscriptionState.Pending);
            }
        }
    }

    public Subscription(String subscriptionId, SubscriptionRequest subReq, Long [] marketIds, long correlationId) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.correlationId  = correlationId;
        this.marketIds = marketIds;
        for (long id : marketIds) {
            marketIdtoState.put(id, SubscriptionState.Pending);
        }
    }

    public Subscription(String subscriptionId, SubscriptionRequest subReq, Integer [] channelIds, long correlationId) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.correlationId  = correlationId;
        this.channelIds = channelIds;
        for (int id : channelIds) {
            channelIdtoState.put(id, SubscriptionState.Pending);
        }
    }

    public Subscription(String subscriptionId, SubscriptionRequest subReq) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.correlationId  = subReq.getCorrelationId();
        List<String> symbols = new ArrayList<>();
        for(SubscriptionRequest.Request req : subReq.getRequestsList()) {
            switch(req.getDataCase()) {
                case SYMBOL:
                    String s = req.getSymbol();
                    symbols.add(s);
                    symboltoState.put(s, SubscriptionState.Pending);
                    break;
                case MARKETID:
                    break;
                case EXCHANGE:
                    break;
                case CHANNELID:
                    break;
                case DATA_NOT_SET:
                    break;
                default:
            }
        }
        this.symbols = symbols.toArray(new String[0]);
    }

    public SubscriptionState getStateSymbol(String symbol) {
        return symboltoState.get(symbol);
    }

    public SubscriptionState getStateMarketId(long marketId) {
        return marketIdtoState.get(marketId);
    }

    public SubscriptionState getStateExchange(String exchange) {
        return exchangetoState.get(exchange);
    }

    public void updateStateSymbol(String symbol, SubscriptionState state) {
        symboltoState.put(symbol, state);
    }

    public void updateStateMarketId(long marketId, SubscriptionState state) {
        marketIdtoState.put(marketId, state);
    }

    public void updateStateExchange(String exchange, SubscriptionState state) {
        exchangetoState.put(exchange,state);
    }

    public void updateStateChannel(int id, SubscriptionState state) {
        channelIdtoState.put(id,state);
    }


    public boolean markSymbolUnsubscribed(String symbol) {
        if (symboltoState.containsKey(symbol)) {
            symboltoState.put(symbol, SubscriptionState.UnSubscribed);
        }
        int unsubscribedCount = 0;
        for (Map.Entry<String, SubscriptionState> e : symboltoState.entrySet()) {
            if (e.getValue() == SubscriptionState.UnSubscribed) {
                unsubscribedCount++;
            }
        }
        return unsubscribedCount == symbols.length;
    }

    public boolean markMarketIdUnsubscribed(long id) {
        if (marketIdtoState.containsKey(id)) {
            marketIdtoState.put(id, SubscriptionState.UnSubscribed);
        }
        int unsubscribedCount = 0;
        for (Map.Entry<Long, SubscriptionState> e : marketIdtoState.entrySet()) {
            if (e.getValue() == SubscriptionState.UnSubscribed) {
                unsubscribedCount++;
            }
        }
        return unsubscribedCount == marketIds.length;
    }

    public boolean markExchangeUnsubscribed(String exchange) {
        if (exchangetoState.containsKey(exchange)) {
            exchangetoState.put(exchange, SubscriptionState.UnSubscribed);
        }
        int unsubscribedCount = 0;
        for (Map.Entry<String, SubscriptionState> e : exchangetoState.entrySet()) {
            if (e.getValue() == SubscriptionState.UnSubscribed) {
                unsubscribedCount++;
            }
        }
        return unsubscribedCount == exchanges.length;
    }

    public boolean markChannelUnsubscribed(int channelId) {
        if (channelIdtoState.containsKey(channelId)) {
            channelIdtoState.put(channelId, SubscriptionState.UnSubscribed);
        }
        int unsubscribedCount = 0;
        for (Map.Entry<Integer, SubscriptionState> e : channelIdtoState.entrySet()) {
            if (e.getValue() == SubscriptionState.UnSubscribed) {
                unsubscribedCount++;
            }
        }
        return unsubscribedCount == channelIds.length;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public SubscriptionRequest getRequest() {
        return request;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(long correlationId) {
        this.correlationId = correlationId;
    }

    public boolean isExchange() {
        return this.exchangeSubscription;
    }

    public void setRequest(SubscriptionRequest request) {
        this.request = request;
    }

    public String[] getSymbols() {
        return symbols;
    }

    public Long[] getMarketIds() {
        return marketIds;
    }

    public String[] getExchanges() {
        return exchanges;
    }

    public Integer [] getChannelIds() {
        return channelIds;
    }

    public Status getResponseStatus() {
        return status;
    }

    public void setResponseStatus(Status status) {
        this.status = status;
    }

    public void setSubscriptionsToUnsubscribed() {
        symboltoState.keySet().forEach( symbol -> symboltoState.put(symbol, SubscriptionState.UnSubscribed));
        marketIdtoState.keySet().forEach( marketId -> marketIdtoState.put(marketId, SubscriptionState.UnSubscribed));
        exchangetoState.keySet().forEach( exchange -> exchangetoState.put(exchange, SubscriptionState.UnSubscribed));
        channelIdtoState.keySet().forEach( channelId -> channelIdtoState.put(channelId, SubscriptionState.UnSubscribed));
    }

}
