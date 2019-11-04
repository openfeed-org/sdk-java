package org.openfeed.client.api.impl;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.SubscriptionRequest;

import java.util.HashMap;
import java.util.Map;

public class Subscription {


    public enum SubscriptionState {
        Pending, Subscribed, UnSubscribed;
    }
    private String subscriptionId;
    private SubscriptionRequest request;
    private  boolean exchangeSubscription;
    private String[] symbols;
    private long[] marketIds;
    private String[] exchanges;
    private int[] channelIds;
    private Map<String, SubscriptionState> symboltoState = new HashMap<>();
    private Map<Long, SubscriptionState> marketIdtoState = new Long2ObjectHashMap<SubscriptionState>();
    private Map<String, SubscriptionState> exchangetoState = new HashMap<>();
    private Map<Integer, SubscriptionState> channelIdtoState = new Int2ObjectHashMap<SubscriptionState>();

    public Subscription(String subscriptionId, SubscriptionRequest subReq, String[] values,
            boolean exchangeSubscription) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.exchangeSubscription = exchangeSubscription;
        if (!exchangeSubscription) {
            this.symbols = values;
            for (String symbol : this.symbols) {
                symboltoState.put(symbol, SubscriptionState.Pending);
            }
        } else {
            this.exchanges = values;
            for (String exchange : exchanges) {
                exchangetoState.put(exchange, SubscriptionState.Pending);
            }
        }
    }

    public Subscription(String subscriptionId, SubscriptionRequest subReq, long[] marketIds) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.marketIds = marketIds;
        for (long id : marketIds) {
            marketIdtoState.put(id, SubscriptionState.Pending);
        }
    }

    public Subscription(String subscriptionId, SubscriptionRequest subReq, int[] channelIds) {
        this.subscriptionId = subscriptionId;
        this.request = subReq;
        this.channelIds = channelIds;
        for (int id : channelIds) {
            channelIdtoState.put(id, SubscriptionState.Pending);
        }
    }

    public Subscription.SubscriptionState getStateSymbol(String symbol) {
        return symboltoState.get(symbol);
    }

    public Subscription.SubscriptionState getStateMarketId(long marketId) {
        return marketIdtoState.get(marketId);
    }

    public Subscription.SubscriptionState getStateExchange(String exchange) {
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
        return unsubscribedCount == symbols.length ? true : false;
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
        return unsubscribedCount == marketIds.length ? true : false;
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
        return unsubscribedCount == exchanges.length ? true : false;
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
        return unsubscribedCount == channelIds.length ? true : false;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public SubscriptionRequest getRequest() {
        return request;
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

    public long[] getMarketIds() {
        return marketIds;
    }

    public String[] getExchanges() {
        return exchanges;
    }

    public int[] getChannelIds() {
        return channelIds;
    }

    public void setSubscriptionsToUnsubscribed() {
        symboltoState.keySet().forEach( symbol -> symboltoState.put(symbol, SubscriptionState.UnSubscribed));
        marketIdtoState.keySet().forEach( marketId -> marketIdtoState.put(marketId, SubscriptionState.UnSubscribed));
        exchangetoState.keySet().forEach( exchange -> exchangetoState.put(exchange, SubscriptionState.UnSubscribed));
        channelIdtoState.keySet().forEach( channelId -> channelIdtoState.put(channelId, SubscriptionState.UnSubscribed));
    }

}
