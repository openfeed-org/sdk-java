package org.openfeed.client.api;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.openfeed.Service;
import org.openfeed.SubscriptionType;
import org.openfeed.client.websocket.Subscription;

import io.netty.channel.ChannelPromise;

public interface OpenfeedClient {

    /**
     * Connects and logins
     */
    void connect();
    void disconnect();
    String getToken();
    void logout();
    boolean isConnected();

    // Instrument
    void instrument(String... symbols);
    void instrumentMarketId(long... marketIds);
    ChannelPromise instrumentChannel(int channelId);
    ChannelPromise instrumentExchange(String exchange);

    // Instrument Cross reference
    void instrumentReference(String... symbols);
    void instrumentReferenceMarketId(long... marketIds);
    ChannelPromise instrumentReferenceExchange(String exchange);
    ChannelPromise instrumentReferenceChannel(int channelId);

  /**
   * Subscribe for symbols.
   * 
   * @param service          Type of service, Realtime, Delayed etc...
   * @param subscriptionType Quote, Depth, etc..
   * @param symbols          List of symbols to subscribe to.
   * @return Unique Id for subscription
   */
    String subscribe(Service service, SubscriptionType subscriptionType, String[] symbols);
    String subscribe(Service service, SubscriptionType subscriptionType, long[] marketIds);
    String subscribeExchange(Service service, SubscriptionType subscriptionType, String[] exchanges);
    String subscribeChannel(Service service, SubscriptionType subscriptionType, int[] channelIds);
    String subscribeSnapshot(String[] symbols, int intervalSec);

    // Un subscribe
    void unSubscribe(Service service, String[] symbols);
    void unSubscribe(Service service, long[] openfeedIds);
    void unSubscribeExchange(Service service, String[] exchanges);
    void unSubscribeChannel(Service service, int[] channelIds);

    Collection<Subscription> getSubscriptions();

    void schedule(Runnable task, long delay, TimeUnit timeUnit);
    void scheduleAtFixedRate(Runnable task, long delay, long interval, TimeUnit timeUnit);


}

