package org.openfeed.client.api;

import org.openfeed.*;
import org.openfeed.client.api.impl.MessageStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketState {
    private static final Logger logger = LoggerFactory.getLogger(MarketState.class);

    private final InstrumentDefinition instrumentDefinition;
    private final long marketId;
    private final String symbol;
    private final String logId;
    private int bookDepth;
    private DepthPriceLevel depthPriceLevel;

    public MarketState(InstrumentDefinition instrumentDefinition) {
        this.instrumentDefinition = instrumentDefinition;
        this.marketId = instrumentDefinition.getMarketId();
        this.symbol = instrumentDefinition.getSymbol();
        this.logId = this.marketId + "/"+this.symbol;
        for (InstrumentDefinition.BookType bookType : instrumentDefinition.getSupportBookTypesList()) {
            switch (bookType) {
                case PRICE_LEVEL_DEPTH:
                    bookDepth = instrumentDefinition.getBookDepth();
                    depthPriceLevel = new DepthPriceLevel(bookDepth);
                    break;
                case ORDER_DEPTH:
                    break;
                default:
            }
        }
    }

    public DepthPriceLevel getDepthPriceLevel() { return this.depthPriceLevel;}

    public void apply(MarketSnapshot snapshot) {
        // clear
        depthPriceLevel.clear();
        for(AddPriceLevel l  : snapshot.getPriceLevelsList()) {
            depthPriceLevel.add(l);
        }
    }

    public void apply(MarketUpdate update) {
        switch(update.getDataCase()) {
            case DEPTHPRICELEVEL:
                handlePriceLevel(update);
                break;
        }
    }

    private void handlePriceLevel(MarketUpdate update) {

        org.openfeed.DepthPriceLevel levels = update.getDepthPriceLevel();
        for (org.openfeed.DepthPriceLevel.Entry level : levels.getLevelsList()) {
            switch (level.getDataCase()) {
                case ADDPRICELEVEL:
                    AddPriceLevel add = level.getAddPriceLevel();
                    depthPriceLevel.add(add);
                    break;
                case DELETEPRICELEVEL:
                    DeletePriceLevel delete = level.getDeletePriceLevel();
                    depthPriceLevel.delete(delete);
                    break;
                case MODIFYPRICELEVEL:
                    ModifyPriceLevel modify = level.getModifyPriceLevel();
                    depthPriceLevel.modify(modify);
                    break;
                case DATA_NOT_SET:
                    break;
            }
        }
    }

}
