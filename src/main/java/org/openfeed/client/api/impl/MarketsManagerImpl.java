package org.openfeed.client.api.impl;

import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.InstrumentDefinition;
import org.openfeed.client.api.MarketState;
import org.openfeed.client.api.MarketsManager;

import java.util.Optional;

public class MarketsManagerImpl implements MarketsManager  {

    private final Long2ObjectHashMap<MarketState> markets = new Long2ObjectHashMap<MarketState>();

    @Override
    public MarketState createMarket(InstrumentDefinition definition) {
        MarketState state = markets.computeIfAbsent(definition.getMarketId(), key -> new MarketState(definition));
        return state;
    }


    @Override
    public Optional<MarketState> getMarket(long marketId) {
        MarketState marketState = this.markets.get(marketId);
        return marketState != null  ? Optional.of(marketState) : Optional.empty();
    }
}
