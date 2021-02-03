package org.openfeed.client.api;

import org.openfeed.InstrumentDefinition;

import java.util.Optional;

public interface MarketsManager {
    MarketState createMarket(InstrumentDefinition definition);

    Optional<MarketState> getMarket(long marketId);
}
