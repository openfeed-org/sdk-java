package org.openfeed.client.api.impl;

public class MessageStats {

    public enum StatType {
        instrument, snapshot, update, bbo, nbbo, trade, ohlc, depth_price, depth_order;
    }

    private long numInstruments;
    private long numSnapshots;
    private long numUpdates;
    private long numBbo;
    private long numNBbo;
    private long numTrades;
    private long numOHLC;
    private long numDepthPrice;
    private long numDepthOrder;
    private String exchangeCode;
    private long defSizeBytes = 0;
    private long snapSizeBytes = 0;
    private long updSizeBytes = 0;

    public MessageStats(String exchangeCode) {
        this.exchangeCode = exchangeCode;
    }

    public MessageStats() {
    }

    public void clear() {
        numInstruments = 0;
        numSnapshots = 0;
        numUpdates = 0;
        numBbo = 0;
        numNBbo = 0;
        numTrades = 0;
        numOHLC = 0;
        numDepthPrice = 0;
        numDepthOrder = 0;
        defSizeBytes = snapSizeBytes = updSizeBytes = 0;
    }


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
        String ec = exchangeCode != null ? "exch: " + exchangeCode + " " : "";
        return ec +
                "inst: " + numInstruments + " snp: " + numSnapshots + " upd: " + numUpdates + " bbo: " + numBbo
                + " nbbo: " + numNBbo
                + " trd: " + numTrades
                + " ohlc: " + numOHLC
                + " depthPrice: " + numDepthPrice
                + " depthOrder: " + numDepthOrder
                + " aveDefSizeBytes: " + (numInstruments > 0 ? (defSizeBytes/numInstruments) : 0)
                + " aveSnapSizeBytes: " + (numSnapshots > 0 ? (snapSizeBytes/numSnapshots) : 0)
                + " aveUpdSizeBytes: " + (numUpdates > 0 ? (updSizeBytes/numUpdates) : 0);
    }


}
