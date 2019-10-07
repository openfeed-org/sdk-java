package org.openfeed.client.websocket;

import java.util.Collection;

import org.openfeed.SubscriptionRequest;

public interface SubscriptionManager {

    void addSubscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols);
    void addSubscription(String subscriptionId, SubscriptionRequest subReq, long[] marketIds);
    void addSubscriptionExchange(String subscriptionId, SubscriptionRequest subReq, String[] exchanges);
    void addSubscriptionChannel(String subscriptionId, SubscriptionRequest subReq, int[] channelIds);

    void removeSubscription(String[] symbols);

    void removeSubscription(long[] marketIds);
    void removeSubscriptionExchange(String[] exchanges);
    void removeSubscriptionChannel(int[] channelIds);
    void removeSubscription(String subscriptionId);

    Collection<Subscription> getSubscriptions();
}
