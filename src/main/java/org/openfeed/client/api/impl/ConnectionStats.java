package org.openfeed.client.api.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ConnectionStats {
    private final MessageStats overallStats = new MessageStats();

    // channel => { exchangeCode => Stats }
    private final Map<Integer,Map<String,MessageStats>> channelToExchangeToMessageStats = new HashMap<>();

    public MessageStats getMessageStats() {
        return this.overallStats;
    }

    public MessageStats getExchangeMessageStats(int channel, String exchangeCode) {
        if (!channelToExchangeToMessageStats.containsKey(channel)) {
            channelToExchangeToMessageStats.put(channel,new HashMap<>());
        }
        return this.channelToExchangeToMessageStats.get(channel)
                .computeIfAbsent(exchangeCode, key -> new MessageStats(channel,exchangeCode));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\nOverall: " + overallStats + "\n");

        for(Map<String,MessageStats> channelStats : channelToExchangeToMessageStats.values()) {
            channelStats.values().forEach(stats ->  sb.append("\t " + stats + "\n"));
        }
        return sb.toString();
    }

    public void clear() {
        overallStats.clear();
        channelToExchangeToMessageStats.clear();
    }
}
