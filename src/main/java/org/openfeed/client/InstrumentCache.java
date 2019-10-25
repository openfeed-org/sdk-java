package org.openfeed.client;

import org.openfeed.InstrumentDefinition;

public interface InstrumentCache {
    void addInstrument(InstrumentDefinition definition);

    InstrumentDefinition getInstrument(long marketId);

    InstrumentDefinition getInstrumentBySeqId(int marketId);

    String getSymbol(long marketId);

    int getTotalNumberOfInstruments();

    void dump();
}
