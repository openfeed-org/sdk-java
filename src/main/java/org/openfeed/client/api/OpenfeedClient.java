package org.openfeed.client.api;

import io.netty.channel.ChannelPromise;
import org.openfeed.*;
import org.openfeed.client.api.impl.Subscription;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface OpenfeedClient {

    /**
     * Blocking call to connect and login.
     */
    void connectAndLogin();
    void disconnect();
    long getCorrelationId();
    String getToken();
    void logout();
    boolean isConnected();
    boolean isReConnect();

    // Sends InstrumentRequest
    void instrumentMarketId(long... marketIds);
    void instrument(String... symbols);
    void instrument(InstrumentRequest request);
    ChannelPromise instrumentChannel(int channelId);
    ChannelPromise instrumentExchange(String exchange);

    // Instrument Cross reference
    void instrumentReference(String... symbols);
    void instrumentReferenceMarketId(long... marketIds);
    ChannelPromise instrumentReferenceExchange(String exchange);
    ChannelPromise instrumentReferenceChannel(int channelId);

    /**
     * Get symbol from marketId, if available in the local cache.
     *
     * @param marketId
     * @return Symbol if available else null
     */
    String getSymbol(long marketId);

    // Sends ExchangeRequest which will list available exchanges.
    void exchangeRequest();

  /**
   * Subscribe by Symbol(s).
   * 
   * @param service          Type of service, Realtime, Delayed etc...
   * @param subscriptionType Quote, Depth, etc..
   * @param symbols          List of symbols to subscribe to.
   * @return Unique Id for subscription
   */
    String subscribe(Service service, SubscriptionType subscriptionType, String[] symbols);

    /**
     * Subscribe by Symbol(s) with multiple subscription types.  The subscription types will apply to all symbols.
     *
     * @param service
     * @param subscriptionTypes
     * @param symbols
     * @return Unique Id for subscription
     */
    String subscribe(Service service, SubscriptionType [] subscriptionTypes, String[] symbols);

    /**
     * Subscribe by Market Id(s).
     *
     * @param service
     * @param subscriptionType
     * @param marketId
     * @return Unique Id for subscription
     */
    String subscribe(Service service, SubscriptionType subscriptionType, long marketId);
    String subscribe(Service service, SubscriptionType subscriptionType, long[] marketIds);
    String subscribe(Service service, SubscriptionType [] subscriptionTypes, long[] marketIds);

    /**
     * Send a pre-build SubscriptionRequest
     * @param request SubscriptionRequest
     */
    void subscribe(SubscriptionRequest request);

    /**
     *
     * @param service
     * @param subscriptionType
     * @param exchanges
     * @return subscriptionId
     */
    String subscribeExchange(Service service, SubscriptionType subscriptionType, String[] exchanges);
    String subscribeExchange(Service service, SubscriptionType [] subscriptionTypes, String[] exchanges);
    String subscribeExchange(Service service, SubscriptionType [] subscriptionTypes, InstrumentDefinition.InstrumentType [] instrumentTypes, String[] exchanges, BulkSubscriptionFilter[] bulkSubscriptionFilters);

    /**
     *
     * @param service
     * @param subscriptionType
     * @param channelIds
     * @return subscriptionId
     */
    String subscribeChannel(Service service, SubscriptionType subscriptionType, int[] channelIds);
    String subscribeChannel(Service service, SubscriptionType [] subscriptionType, int[] channelIds);
    String subscribeChannel(Service service, SubscriptionType [] subscriptionType, InstrumentDefinition.InstrumentType [] instrumentTypes, int[] channelIds, BulkSubscriptionFilter[] bulkSubscriptionFilters);

    /**
     * Subscribe for Realtime Snapshots at specific interval
     * @param  symbols Symbols
     * @param  intervalSec  if 0 then only the latest snapshot is sent, otherwise send at specific interval.
     * @return  subscriptionId
     */
    String subscribeSnapshot(String[] symbols, int intervalSec);
    /**
     * Subscribe for Snapshots
     *
     * @param service  Realtime, Delayed
     * @param symbols  Symbols
     * @param intervalSec  if 0 then only the latest snapshot is sent, otherwise send at specific interval.
     * @return  subscriptionId
     */
    String subscribeSnapshot(Service service, String[] symbols, int intervalSec);
    /**
     * Subscribes for Snapshot at the requested service type
     *
     * @param service Service Type, can be REAL_TIME_SNAPSHOT or DELAYED_SNAPSHOT
     * @param subscriptionType  Subscription Type, can be Cumulative Volume
     * @param symbols  Symbols
     * @param intervalSec  if 0 then only the latest snapshot is sent, otherwise send at specific interval.
     * @return subscriptionId
     */
    String subscribeSnapshot(Service service, SubscriptionType subscriptionType, String[] symbols, int intervalSec);
    String subscribeSnapshot(Service service, SubscriptionType[] subscriptionType, String[] symbols, int intervalSec);
    String subscribeSnapshot(Service service, SubscriptionType subscriptionType, long marketId, int intervalSec);


    // Un subscribe
    void unSubscribe(Service service, String[] symbols);
    void unSubscribe(Service service,SubscriptionType subscriptionType, String[] symbols);
    //
    void unSubscribe(Service service, long[] openfeedIds);
    void unSubscribe(Service service,SubscriptionType subscriptionType, long[] openfeedIds);
    //
    void unSubscribeExchange(Service service, String[] exchanges);
    void unSubscribeExchange(Service service, SubscriptionType subscriptionType, String[] exchanges);
    //
    void unSubscribeChannel(Service service, int[] channelIds);

    /**
     * Returns all subscriptions.
     *
     * @return Subscriptions
     */
    Collection<Subscription> getSubscriptions();

    /**
     * Get subscription by Id
     *
     * @param subscriptionId Subscription Id returned by subscribe* methods.
     * @return Subscription
     */
    Subscription getSubscription(String subscriptionId);

    void schedule(Runnable task, long delay, TimeUnit timeUnit);
    void scheduleAtFixedRate(Runnable task, long delay, long interval, TimeUnit timeUnit);


}

