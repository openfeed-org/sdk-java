package org.openfeed.client.api.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ConnectionStats {
    private MessageStats overallStats = new MessageStats();

    private Map<String, MessageStats>[] channelToExchangeToMessageStats = new Map[256];

    public MessageStats getMessageStats() {
        return this.overallStats;
    }

    public MessageStats getExchangeMessageStats(int channel, String exchangeCode) {
        if (channelToExchangeToMessageStats[channel] == null) {
            channelToExchangeToMessageStats[channel] = new HashMap<>();
        }
        return this.channelToExchangeToMessageStats[channel]
                .computeIfAbsent(exchangeCode, key -> new MessageStats(channel,exchangeCode));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\nOverall: " + overallStats + "\n");

        Arrays.stream(channelToExchangeToMessageStats).filter(m -> m != null)
                .forEach( m -> m.values().forEach(stats ->
            sb.append("\t " + stats + "\n") ));

        return sb.toString();
    }

    public void clear() {
        overallStats.clear();
        Arrays.stream(channelToExchangeToMessageStats).filter(m -> m != null).forEach( m -> m.clear());
    }
}
