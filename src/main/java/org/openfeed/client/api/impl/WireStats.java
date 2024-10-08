package org.openfeed.client.api.impl;

import org.HdrHistogram.Histogram;

public class WireStats {
    private static final int MB = 1000 * 1000;
    private long packetsReceived;
    private long messagesPerPacket;
    private long bytesReceived;
    private long bitsReceived;
    private long pingsReceived;
    private long pongsReceived;
    private Histogram bitsReceivedHistogram= new Histogram(3600000000L, 2);

    public void update(long bytesReceived,int numMessages) {
        this.packetsReceived++;
        this.messagesPerPacket += numMessages;
        this.bytesReceived += bytesReceived;
        this.bitsReceived += bytesReceived * 8;
        bitsReceivedHistogram.recordValue(this.bitsReceived/MB);
    }

    public void reset() {
       this.packetsReceived = this.messagesPerPacket= this.bytesReceived = this.bitsReceived = 0;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public long getBitsReceived() {
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
        return "Wire: Kbytes/sec = " + bytesReceived / 1000 + ", Mean Mbps = " +
                bitsReceivedHistogram.getMean() + ", Stddev Mbps = "
                + bitsReceivedHistogram.getStdDeviation()
                + ", Max Mbps = " + bitsReceivedHistogram.getMaxValue()
                + ", packets = "+packetsReceived + ", avePacketSizeBytes = "+ (packetsReceived > 0 ? (bytesReceived/packetsReceived) : 0)
                + ", aveMsgs/packet = "+ (packetsReceived > 0 ? (messagesPerPacket/packetsReceived) : 0)
                + ", pings = "+ this.pingsReceived + " pongs = "+ this.pongsReceived;
    }
}
