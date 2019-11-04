package org.openfeed.client.api.impl;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.Result;
import org.openfeed.SubscriptionRequest;
import org.openfeed.SubscriptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionManagerImpl implements SubscriptionManager {
    private static final Logger log = LoggerFactory.getLogger(SubscriptionManager.class);

    //
    private Map<String, Subscription> subscriptionIdToSubscription = new ConcurrentHashMap<>();
    private Map<Long, Subscription> correlationIdToSubscription = new Long2ObjectHashMap<>();
    //
    private Map<String, Subscription> symbolToSubscription = new ConcurrentHashMap<>();
    private Map<Long, Subscription> marketIdToSubscription = new Long2ObjectHashMap<Subscription>();
    private Map<String, Subscription> exchangeToSubscription = new ConcurrentHashMap<>();
    private Map<Integer, Subscription> channelIdToSubscription = new Int2ObjectHashMap<Subscription>();

    @Override
    public boolean isSubscribed(String symbol) {
        Subscription subscription = symbolToSubscription.get(symbol);
        if(subscription != null) {
            switch(subscription.getStateSymbol(symbol)) {
                case UnSubscribed:
                case Pending:
                    return false;
                case Subscribed:
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSubscribed(long marketId) {
        Subscription subscription = marketIdToSubscription.get(marketId);
        if(subscription != null) {
            switch(subscription.getStateMarketId(marketId)) {
                case UnSubscribed:
                case Pending:
                    return false;
                case Subscribed:
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSubscribedExchange(String exchange) {
        Subscription subscription = exchangeToSubscription.get(exchange);
        if(subscription != null) {
            switch(subscription.getStateExchange(exchange)) {
                case UnSubscribed:
                case Pending:
                    return false;
                case Subscribed:
                    return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSubscribedChannel(int channel) {
        return channelIdToSubscription.containsKey(channel);
    }

    @Override
    public void addSubscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols) {
        Subscription subscription = new Subscription(subscriptionId, subReq, symbols, false);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        correlationIdToSubscription.put(subReq.getCorrelationId(), subscription);
        for (String s : symbols) {
            symbolToSubscription.put(s, subscription);
        }
    }

    @Override
    public void addSubscription(String subscriptionId, SubscriptionRequest subReq, long[] marketIds) {
        Subscription subscription = new Subscription(subscriptionId, subReq, marketIds);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        correlationIdToSubscription.put(subReq.getCorrelationId(), subscription);
        for (long id : marketIds) {
            marketIdToSubscription.put(id, subscription);
        }
    }

    @Override
    public void addSubscriptionExchange(String subscriptionId, SubscriptionRequest subReq, String[] exchanges) {
        Subscription subscription = new Subscription(subscriptionId, subReq, exchanges, true);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        correlationIdToSubscription.put(subReq.getCorrelationId(), subscription);
        for (String exchange : exchanges) {
            exchangeToSubscription.put(exchange, subscription);
        }
    }

    @Override
    public void addSubscriptionChannel(String subscriptionId, SubscriptionRequest subReq, int[] channelIds) {
        Subscription subscription = new Subscription(subscriptionId, subReq, channelIds);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        correlationIdToSubscription.put(subReq.getCorrelationId(), subscription);
        for (int id : channelIds) {
            channelIdToSubscription.put(id, subscription);
        }
    }

    @Override
    public void removeSubscription(String symbol) {
        removeSubscription(new String[] {symbol});
    }

    @Override
    public void removeSubscription(String[] symbols) {
        for (String symbol : symbols) {
            Subscription sub = symbolToSubscription.remove(symbol);
            if (sub != null) {
                boolean allUnsubscribed = sub.markSymbolUnsubscribed(symbol);
                if (allUnsubscribed) {
                    // Remove Subscription Request if no more symbols are subscribed.
                    removeSubscriptionById(sub.getSubscriptionId());
                }
            }
        }
    }

    @Override
    public void removeSubscriptionExchange(String[] exchanges) {
        for (String exchange : exchanges) {
            Subscription sub = exchangeToSubscription.remove(exchange);
            if (sub != null) {
                boolean allUnsubscribed = sub.markExchangeUnsubscribed(exchange);
                if (allUnsubscribed) {
                    // Remove Subscription Request if no more symbols are subscribed.
                    removeSubscriptionById(sub.getSubscriptionId());
                }
            }
        }
    }

    @Override
    public void removeSubscriptionChannel(int[] channelIds) {
        for (int channelId : channelIds) {
            Subscription sub = channelIdToSubscription.remove(channelId);
            if (sub != null) {
                boolean allUnsubscribed = sub.markChannelUnsubscribed(channelId);
                if (allUnsubscribed) {
                    // Remove Subscription Request if no more symbols are subscribed.
                    removeSubscriptionById(sub.getSubscriptionId());
                }
            }
        }
    }

    @Override
    public void removeSubscription(long[] marketIds) {
        for (long id : marketIds) {
            Subscription sub = marketIdToSubscription.remove(id);
            if (sub != null) {
                boolean allUnsubscribed = sub.markMarketIdUnsubscribed(id);
                if (allUnsubscribed) {
                    removeSubscriptionById(sub.getSubscriptionId());
                }
            }
        }
    }


    @Override
    public Collection<Subscription> getSubscriptions() {
        return this.subscriptionIdToSubscription.values();
    }


    @Override
    public void removeSubscriptionById(String subscriptionId) {
        Subscription subscription = subscriptionIdToSubscription.remove(subscriptionId);
        correlationIdToSubscription.remove(subscription.getRequest().getCorrelationId());
        if(subscription.getSymbols() != null) {
            Arrays.stream(subscription.getSymbols()).forEach( s -> symbolToSubscription.remove(s));
         }
        else if(subscription.getMarketIds() != null ) {
            Arrays.stream(subscription.getMarketIds()).forEach( id -> marketIdToSubscription.remove(id));
        }
        else if(subscription.getExchanges() != null ) {
            Arrays.stream(subscription.getExchanges()).forEach( exch -> exchangeToSubscription.remove(exch));
        }
        else if(subscription.getChannelIds() != null ) {
            Arrays.stream(subscription.getChannelIds()).forEach( id -> channelIdToSubscription.remove(id));
        }
    }

    public void updateSubscriptionState(SubscriptionResponse subscriptionResponse) {
        Subscription subscription = this.correlationIdToSubscription.get(subscriptionResponse.getCorrelationId());
        if (subscription == null) {
            log.warn("No subscription found for: {}", subscriptionResponse);
            return;
        }
        boolean success = subscriptionResponse.getStatus().getResult() == Result.SUCCESS ? true : false;
        if(subscription.getSymbols() != null) {
            if(success) {
                Arrays.stream(subscription.getSymbols()).forEach( s -> subscription.updateStateSymbol(s,Subscription.SubscriptionState.Subscribed));
            }
            else {
                Arrays.stream(subscription.getSymbols()).forEach( s -> subscription.updateStateSymbol(s,Subscription.SubscriptionState.UnSubscribed));
            }
        }
        else if(subscription.getMarketIds() != null ) {
            if(success) {
                Arrays.stream(subscription.getMarketIds()).forEach( id -> subscription.updateStateMarketId(id,Subscription.SubscriptionState.Subscribed));
            }
            else {
                Arrays.stream(subscription.getMarketIds()).forEach( id -> subscription.updateStateMarketId(id,Subscription.SubscriptionState.UnSubscribed));
            }
        }
        else if(subscription.getExchanges() != null ) {
            if(success) {
                Arrays.stream(subscription.getExchanges()).forEach( exch -> subscription.updateStateExchange(exch,Subscription.SubscriptionState.Subscribed));
            }
            else {
                Arrays.stream(subscription.getExchanges()).forEach( exch -> subscription.updateStateExchange(exch,Subscription.SubscriptionState.UnSubscribed));
            }

        }
        else if(subscription.getChannelIds() != null ) {
            if(success) {
                Arrays.stream(subscription.getChannelIds()).forEach( id -> subscription.updateStateChannel(id,Subscription.SubscriptionState.Subscribed));
            }
            else {
                Arrays.stream(subscription.getChannelIds()).forEach( id -> subscription.updateStateChannel(id,Subscription.SubscriptionState.UnSubscribed));
            }

        }
    }

    public void setAllSubscriptionsUnsubcribed() {
        this.subscriptionIdToSubscription.forEach( (id,subscription) -> subscription.setSubscriptionsToUnsubscribed());
    }
}
