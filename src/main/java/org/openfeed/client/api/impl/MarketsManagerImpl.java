package org.openfeed.client.api.impl;

import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.InstrumentDefinition;
import org.openfeed.client.api.MarketState;
import org.openfeed.client.api.MarketsManager;

public class MarketsManagerImpl implements MarketsManager  {

    private final Long2ObjectHashMap<MarketState> markets = new Long2ObjectHashMap<MarketState>();


    @Override
    public MarketState createMarket(InstrumentDefinition definition) {
        MarketState state = markets.computeIfAbsent(definition.getMarketId(), key -> new MarketState(definition));
        return state;
    }

    @Override
    public MarketState getMarket(long marketId) {
        return this.markets.get(marketId);
    }
}
