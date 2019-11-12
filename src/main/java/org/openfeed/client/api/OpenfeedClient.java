package org.openfeed.client.api;

import io.netty.channel.ChannelPromise;
import org.openfeed.Service;
import org.openfeed.SubscriptionType;
import org.openfeed.client.api.impl.Subscription;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface OpenfeedClient {

    /**
     * Blocking call to connect and login.
     */
    void connectAndLogin();
    void disconnect();
    String getToken();
    void logout();
    boolean isConnected();
    boolean isReConnect();

    // Sends InstrumentRequest
    void instrumentMarketId(long... marketIds);
    void instrument(String... symbols);
    ChannelPromise instrumentChannel(int channelId);
    ChannelPromise instrumentExchange(String exchange);

    // Instrument Cross reference
    void instrumentReference(String... symbols);
    void instrumentReferenceMarketId(long... marketIds);
    ChannelPromise instrumentReferenceExchange(String exchange);
    ChannelPromise instrumentReferenceChannel(int channelId);

    // Sends ExchangeRequest which will list available exchanges.
    void exchangeRequest();

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

