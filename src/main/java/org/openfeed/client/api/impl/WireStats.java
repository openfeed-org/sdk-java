package org.openfeed.client.api.impl;


public class WireStats {
    public  static final int MB = 1000 * 1000;

    private long packetsReceived;
    private long messagesPerPacket;
    private double bytesReceived;
    private double bitsReceived;
    private long pingsReceived;
    private long pongsReceived;

    public void update(long bytesReceived,int numMessages) {
        this.packetsReceived++;
        this.messagesPerPacket += numMessages;
        this.bytesReceived += bytesReceived;
        this.bitsReceived += (bytesReceived * 8);
    }

    public void reset() {
       this.packetsReceived = this.messagesPerPacket= 0;
       this.bytesReceived = this.bitsReceived = 0.0;
    }

    public double getBytesReceived() {
        return bytesReceived;
    }

    public double getBitsReceived() {
        return bitsReceived;
    }

    public long getPongsReceived() {
        return pongsReceived;
    }

    public void incrPongsReceived() {
        this.pongsReceived++;
    }

    public long getPingsReceived() {
        return pingsReceived;
    }

    public void incrPingsReceived() {
        this.pingsReceived++;
    }

    public String toString() {
        return "Wire: Mbytes = " + bytesReceived / MB
                + ", packets = "+packetsReceived + ", avePacketSizeBytes = "+ (packetsReceived > 0 ? (bytesReceived/packetsReceived) : 0)
                + ", aveMsgs/packet = "+ (packetsReceived > 0 ? (messagesPerPacket/packetsReceived) : 0)
                + ", pings = "+ this.pingsReceived + " pongs = "+ this.pongsReceived;
    }
}
