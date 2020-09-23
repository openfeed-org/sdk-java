package org.openfeed.client.api.impl;

import org.openfeed.SubscriptionRequest;

import java.util.Collection;

public interface SubscriptionManager {

    boolean isSubscribed(String symbol);
    boolean isSubscribed(long marketId);
    boolean isSubscribedExchange(String exchange);
    boolean isSubscribedChannel(int channel);

    void addSubscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols,long correlationId);
    void addSubscription(String subscriptionId, SubscriptionRequest subReq, Long [] marketIds,long correlationId);
    void addSubscriptionExchange(String subscriptionId, SubscriptionRequest subReq, String[] exchanges,long correlationId);
    void addSubscriptionChannel(String subscriptionId, SubscriptionRequest subReq, Integer [] channelIds,long correlationId);

    void removeSubscriptionById(String subscriptionId);
    void removeSubscription(String symbol);
    void removeSubscription(String[] symbols);
    void removeSubscription(long[] marketIds);
    void removeSubscriptionExchange(String[] exchanges);
    void removeSubscriptionChannel(int[] channelIds);

    Subscription getSubscription(String subscriptionId);
    Collection<Subscription> getSubscriptions();
}
