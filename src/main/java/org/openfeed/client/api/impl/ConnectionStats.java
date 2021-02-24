package org.openfeed.client.api.impl;

import java.util.Comparator;
import java.util.Map;

import org.agrona.collections.Object2ObjectHashMap;

public class ConnectionStats {

    private MessageStats overallStats = new MessageStats();
    // Key = channelId:exchangeCode
    private Map<String, MessageStats> exchangeToMessageStats = new Object2ObjectHashMap<String, MessageStats>();

    public MessageStats getMessageStats() {
        return this.overallStats;
    }

    public MessageStats getExchangeMessageStats(int channel,String exchangeCode) {
        return this.exchangeToMessageStats.computeIfAbsent(MessageStats.makeExchangeKey(channel,exchangeCode), key -> new MessageStats(channel,exchangeCode));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\nOverall: " + overallStats + "\n");
        exchangeToMessageStats.values().stream().sorted(Comparator.comparing(MessageStats::getId)).forEach((stats) -> {
            sb.append("\t " + stats + "\n");
        });
        return sb.toString();
    }

    public void clear() {
        overallStats.clear();
        exchangeToMessageStats.forEach( (code, stats) -> {
            stats.clear();
        });
    }

}
