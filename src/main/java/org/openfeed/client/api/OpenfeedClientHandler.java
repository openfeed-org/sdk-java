package org.openfeed.client.api;

import io.netty.channel.ChannelPromise;
import org.openfeed.*;
import org.openfeed.client.api.impl.ConnectionStats;

public interface OpenfeedClientHandler {
    // Request Responses
    void onLoginResponse(LoginResponse loginResponse);
    void onLogoutResponse(LogoutResponse logoutResponse);
    void onInstrumentResponse(InstrumentResponse instrumentResponse);
    void onInstrumentReferenceResponse(InstrumentReferenceResponse instrumentReferenceResponse);
    // Promise to trigger end of instrument and/or instrument reference downloads
    void setInstrumentPromise(ChannelPromise promise);
    void onExchangeResponse(ExchangeResponse exchangeResponse);
    void onSubscriptionResponse(SubscriptionResponse subscriptionResponse);

    // Streaming
    void onMarketStatus(MarketStatus marketStatus);
    void onHeartBeat(HeartBeat hb);
    void onInstrumentDefinition(InstrumentDefinition definition);
    void onMarketSnapshot(MarketSnapshot snapshot);
    void onMarketUpdate(MarketUpdate update);
    void onVolumeAtPrice(VolumeAtPrice cumulativeVolume);
    void onOhlc(Ohlc ohlc);
    void onInstrumentAction(InstrumentAction instrumentAction);
    void onListSubscriptionsResponse(ListSubscriptionsResponse listSubscriptionsResponse);

    ConnectionStats getConnectionStats();

}
