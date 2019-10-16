package org.openfeed.client;

import java.util.Map;

import org.agrona.collections.Object2ObjectHashMap;

public class ConnectionStats {

    private MessageStats overallStats = new MessageStats();
    private Map<String, MessageStats> exchangeToMessageStats = new Object2ObjectHashMap<String, MessageStats>();

    public MessageStats getMessageStats() {
        return this.overallStats;
    }

    public MessageStats getExchangeMessageStats(String exchangeCode) {
        return this.exchangeToMessageStats.computeIfAbsent(exchangeCode, key -> new MessageStats(key));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\nOverall: " + overallStats + "\n");
        exchangeToMessageStats.forEach((ec, stats) -> {
            sb.append("\t " + stats.toString() + "\n");
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
