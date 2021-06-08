package org.openfeed.client.api;

import org.openfeed.InstrumentDefinition;
import java.util.Collection;

public interface InstrumentCache {


    void addInstrument(InstrumentDefinition definition);

    InstrumentDefinition getInstrument(long marketId);

    InstrumentDefinition getInstrumentBySeqId(int marketId);

    String getSymbol(long marketId);

    int getTotalNumberOfInstruments();

    Collection<InstrumentDefinition> getAllInstruments();

    Integer[] getChannelIds();

    Collection<InstrumentDefinition> getInstrumentsByChannel(int channelId);

    void dump();

    String VENDOR_BARCHART = "Barchart";
     static String getDdfSymbol(InstrumentDefinition i) {
        if (i.getSymbolsCount() > 0) {
            for (InstrumentDefinition.Symbol s : i.getSymbolsList()) {
                if (s.getVendor().equals(VENDOR_BARCHART)) {
                    return s.getSymbol();
                }
            }
        }
        return null;
    }

}
