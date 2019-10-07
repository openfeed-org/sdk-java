package org.openfeed.client.websocket;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.SubscriptionRequest;

public class SubscriptionManagerImpl implements SubscriptionManager {
    private Map<String, Subscription> subscriptionIdToSubscription = new ConcurrentHashMap<>();
    //
    private Map<String, Subscription> symbolToSubscription = new ConcurrentHashMap<>();
    private Map<Long, Subscription> marketIdToSubscription = new Long2ObjectHashMap<Subscription>();
    private Map<String, Subscription> exchangeToSubscription = new ConcurrentHashMap<>();
    private Map<Integer, Subscription> channelIdToSubscription = new Int2ObjectHashMap<Subscription>();

    @Override
    public void addSubscription(String subscriptionId, SubscriptionRequest subReq, String[] symbols) {
        Subscription subscription = new Subscription(subscriptionId, subReq, symbols, false);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        for (String s : symbols) {
            symbolToSubscription.put(s, subscription);
        }
    }

    @Override
    public void addSubscription(String subscriptionId, SubscriptionRequest subReq, long[] marketIds) {
        Subscription subscription = new Subscription(subscriptionId, subReq, marketIds);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        for (long id : marketIds) {
            marketIdToSubscription.put(id, subscription);
        }
    }

    @Override
    public void addSubscriptionExchange(String subscriptionId, SubscriptionRequest subReq, String[] exchanges) {
        Subscription subscription = new Subscription(subscriptionId, subReq, exchanges, true);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        for (String exchange : exchanges) {
            exchangeToSubscription.put(exchange, subscription);
        }
    }

    @Override
    public void addSubscriptionChannel(String subscriptionId, SubscriptionRequest subReq, int[] channelIds) {
        Subscription subscription = new Subscription(subscriptionId, subReq, channelIds);
        subscriptionIdToSubscription.put(subscriptionId, subscription);
        for (int id : channelIds) {
            channelIdToSubscription.put(id, subscription);
        }
    }

    @Override
    public void removeSubscription(String[] symbols) {
        for (String symbol : symbols) {
            Subscription sub = symbolToSubscription.remove(symbol);
            if (sub != null) {
                boolean allUnsubscribed = sub.markSymbolUnsubscribed(symbol);
                if (allUnsubscribed) {
                    // Remove Subscription Request if no more symbols are subscribed.
                    subscriptionIdToSubscription.remove(sub.getSubscriptionId());
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
                    subscriptionIdToSubscription.remove(sub.getSubscriptionId());
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
                    subscriptionIdToSubscription.remove(sub.getSubscriptionId());
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
                    subscriptionIdToSubscription.remove(sub.getSubscriptionId());
                }
            }
        }
    }


    @Override
    public Collection<Subscription> getSubscriptions() {
        return this.subscriptionIdToSubscription.values();
    }




    @Override
    public void removeSubscription(String subscriptionId) {
        subscriptionIdToSubscription.remove(subscriptionId);
    }
}
