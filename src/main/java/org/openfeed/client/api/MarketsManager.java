package org.openfeed.client.api;

import org.openfeed.InstrumentDefinition;

public interface MarketsManager {
    MarketState createMarket(InstrumentDefinition definition);

    MarketState getMarket(long marketId);
}
