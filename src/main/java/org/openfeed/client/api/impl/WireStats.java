package org.openfeed.client.api.impl;

import org.HdrHistogram.Histogram;

public class WireStats {
    private static final int MB = 1000 * 1000;
    private long bytesReceived;
    private long bitsReceived;
    private Histogram bitsReceivedHistogram= new Histogram(3600000000L, 2);

    public void update(long bytesReceived) {
        this.bytesReceived += bytesReceived;
        this.bitsReceived += bytesReceived * 8;
        bitsReceivedHistogram.recordValue(this.bitsReceived/MB);
    }

    public void reset() {
        this.bytesReceived = this.bitsReceived = 0;
    }

    public long getBytesReceived() {
        return bytesReceived;
    }

    public long getBitsReceived() {
        return bitsReceived;
    }

    public String toString() {
        return "Wire: Kbytes/sec = " + bytesReceived / 1000 + ", Mean Mbps = " +
                bitsReceivedHistogram.getMean() + ", Stddev Mbps = "
                + bitsReceivedHistogram.getStdDeviation()
                + ", Max Mbps = " + bitsReceivedHistogram.getMaxValue();
    }
}
