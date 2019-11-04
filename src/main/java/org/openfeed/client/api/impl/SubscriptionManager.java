package org.openfeed.client.api.impl;

import org.openfeed.SubscriptionRequest;

import java.util.Collection;

public interface SubscriptionManager {

    boolean isSubscribed(String symbol);
    boolean isSubscribed(long marketId);
    boolean isSubscribedExchange(String exchange);
    boolean isSubscribedChannel(int channel);

    void addSubscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols);
    void addSubscription(String subscriptionId, SubscriptionRequest subReq, long[] marketIds);
    void addSubscriptionExchange(String subscriptionId, SubscriptionRequest subReq, String[] exchanges);
    void addSubscriptionChannel(String subscriptionId, SubscriptionRequest subReq, int[] channelIds);

    void removeSubscriptionById(String subscriptionId);
    void removeSubscription(String symbol);
    void removeSubscription(String[] symbols);
    void removeSubscription(long[] marketIds);
    void removeSubscriptionExchange(String[] exchanges);
    void removeSubscriptionChannel(int[] channelIds);

    Collection<Subscription> getSubscriptions();
}
