package org.openfeed.client.api.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.Object2LongHashMap;
import org.openfeed.InstrumentDefinition;
import org.openfeed.client.api.InstrumentCache;
import org.openfeed.client.api.OpenfeedClientHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentCacheImpl implements InstrumentCache {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientHandler.class);

    private Map<String, Long> symbolToMarketId = new Object2LongHashMap<String>(Long.MIN_VALUE);
    private Map<Long, InstrumentDefinition> marketIdToInstrument = new Long2ObjectHashMap<InstrumentDefinition>();
    private Map<Integer, List<InstrumentDefinition>> channelIdToInstrument = new Int2ObjectHashMap<List<InstrumentDefinition>>();
    private Map<String, Map<Long, InstrumentDefinition>> exchangeCodeToInstruments = new ConcurrentHashMap<String, Map<Long, InstrumentDefinition>>();
    private Map<Integer, InstrumentDefinition> sequentialIdToInstrument = new Int2ObjectHashMap<>();
    private int sequentialId = -1;

    @Override
    public void addInstrument(InstrumentDefinition definition) {
        this.symbolToMarketId.put(definition.getSymbol(), definition.getMarketId());
        this.marketIdToInstrument.put(definition.getMarketId(), definition);
        // By Channel
        List<InstrumentDefinition> l = this.channelIdToInstrument.computeIfAbsent(definition.getChannel(),k -> new ArrayList<InstrumentDefinition>());
        l.add(definition);
        // By Exchange
        String ecode = definition.getExchangeCode();
        Map<Long, InstrumentDefinition> exchangeInsruments = exchangeCodeToInstruments.get(ecode);
        if (exchangeInsruments == null) {
            exchangeInsruments = new Long2ObjectHashMap<InstrumentDefinition>();
            exchangeCodeToInstruments.put(ecode, exchangeInsruments);
        }
        exchangeInsruments.put(definition.getMarketId(), definition);
        // By Sequential Id
        sequentialId++;
        sequentialIdToInstrument.put(sequentialId, definition);
    }

    @Override
    public int getTotalNumberOfInstruments() {
        return this.sequentialIdToInstrument.size();
    }

    @Override
    public Collection<InstrumentDefinition> getInstrumentsByChannel(int channelId) {
        return this.channelIdToInstrument.get(channelId);
    }

    @Override
    public Collection<InstrumentDefinition> getAllInstruments() {
        return this.marketIdToInstrument.values();
    }

    @Override
    public Integer[] getChannelIds() {
        return this.channelIdToInstrument.keySet().toArray(new Integer[this.channelIdToInstrument.size()]);
    }

    @Override
    public InstrumentDefinition getInstrument(long marketId) {
        return this.marketIdToInstrument.get(marketId);
    }

    @Override
    public void dump() {
        log.info("TotalInstruments: {}", sequentialIdToInstrument.size());
                
        exchangeCodeToInstruments.forEach((exchangeCode, instruments) -> {
            log.info("{}: numInst: {}", exchangeCode, instruments.size());
        });
    }

    @Override
    public InstrumentDefinition getInstrumentBySeqId(int seqId) {
        return this.sequentialIdToInstrument.get(seqId);
    }

    @Override
    public String getSymbol(long marketId) {
        InstrumentDefinition def = marketIdToInstrument.get(marketId);
        return def != null ? def.getSymbol() : "";
    }

}
