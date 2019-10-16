package org.openfeed.client;

public class MessageStats {

    public enum StatType {
        instrument, snapshot, update, bbo, nbbo, trade
    }

    private long numInstruments;
    private long numSnapshots;
    private long numUpdates;
    private long numBbo;
    private long numNBbo;
    private long numTrades;
    private String exchangeCode;

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

    @Override
    public String toString() {
        String ec = exchangeCode != null ? "exch: " + exchangeCode + " " : "";
        return ec +
                "inst: " + numInstruments + " snp: " + numSnapshots + " upd: " + numUpdates + " bbo: " + numBbo
                + " nbbo: " + numNBbo
                + " trd: " + numTrades;
    }


}
