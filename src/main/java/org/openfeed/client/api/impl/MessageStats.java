package org.openfeed.client.api.impl;

public class MessageStats {

    public enum StatType {
        instrument, snapshot, update, bbo, nbbo, trade, trade_correction, trade_cancel, ohlc, depth_price, depth_order;
    }

    private int channel;
    private  String exchangeCode;
    private String id = "";
    //
    private long numInstruments;
    private long numSnapshots;
    private long numUpdates;
    private long numBbo;
    private long numNBbo;
    private long numTrades;
    private long numTradeCorrection;
    private long numTradeCancel;
    private long numOHLC;
    private long numDepthPrice;
    private long numDepthOrder;
    private long defSizeBytes = 0;
    private long snapSizeBytes = 0;
    private long updSizeBytes = 0;


    public static String makeExchangeKey(int channel, String exchange) { return channel + ":"+ exchange; }

    public MessageStats(int channel,String exchangeCode) {
        this.channel = channel;
        this.exchangeCode = exchangeCode;
        this.id = channel + ":"+exchangeCode;
    }

    public MessageStats() {
    }

    public void clear() {
        numInstruments = 0;
        numSnapshots = 0;
        numUpdates = 0;
        numBbo = 0;
        numNBbo = 0;
        numTrades = numTradeCorrection = numTradeCancel = 0;
        numOHLC = 0;
        numDepthPrice = 0;
        numDepthOrder = 0;
        defSizeBytes = snapSizeBytes = updSizeBytes = 0;
    }

    public int getChannel() { return this.channel; }
    public String getExchangeCode() {return this.exchangeCode; }
    public String getId() { return this.id; }

    public void incrInstruments() {
        this.numInstruments++;
    }

    public void incrSnapshots() {
        this.numSnapshots++;
    }

    public void incrUpdates() {
        this.numUpdates++;
    }

    public void incrBbo() {
        this.numBbo++;
    }

    public void incrTrades() {
        this.numTrades++;
    }

    public void incrTradeCorrection() {
        this.numTradeCorrection++;
    }

    public void incrTradeCancel() {
        this.numTradeCancel++;
    }
    public void incrNBbo() {
        this.numNBbo++;
    }

    public void incrOHLC() {
        this.numOHLC++;
    }

    public void incrDepthPrice() {
        this.numDepthPrice++;
    }

    public void incrDepthOrder() {
        this.numDepthOrder++;
    }

    public long getNumInstruments() {
        return numInstruments;
    }

    public long getNumSnapshots() {
        return numSnapshots;
    }

    public long getNumUpdates() {
        return numUpdates;
    }

    public long getNumBbo() {
        return numBbo;
    }

    public long getNumNBbo() {
        return numNBbo;
    }

    public long getNumTrades() {
        return numTrades;
    }
    
    public long getNumTradeCorrection() {
        return numTradeCorrection;
    }
    
    public long getNumTradeCAncel() {
        return numTradeCancel;
    }
    public long getNumOHLC() {
        return numOHLC;
    }

    public long getNumDepthPrice() {
        return numDepthPrice;
    }

    public long getNumDepthOrder() {
        return numDepthOrder;
    }

    public void addDefSizeBytes(int v) {
        this.defSizeBytes += v;
    }

    public void addSnapSizeBytes(int v) {
        this.snapSizeBytes += v;
    }

    public void addUpdSizeBytes(int v) {
        this.updSizeBytes += v;
    }

    @Override
    public String toString() {
        String ec = getId();
        return ec +
                " inst: " + numInstruments + " snp: " + numSnapshots + " upd: " + numUpdates + " bbo: " + numBbo
                + " nbbo: " + numNBbo
                + " trd: " + numTrades
                + " trdCorr: " + numTradeCorrection
                + " trdCancel: " + numTradeCancel
                + " ohlc: " + numOHLC
                + " depthPrice: " + numDepthPrice
                + " depthOrder: " + numDepthOrder
                + " aveDefSizeBytes: " + (numInstruments > 0 ? (defSizeBytes/numInstruments) : 0)
                + " aveSnapSizeBytes: " + (numSnapshots > 0 ? (snapSizeBytes/numSnapshots) : 0)
                + " aveUpdSizeBytes: " + (numUpdates > 0 ? (updSizeBytes/numUpdates) : 0);
    }


}
