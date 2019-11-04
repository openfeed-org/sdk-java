package org.openfeed.client;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.openfeed.BestBidOffer;
import org.openfeed.HeartBeat;
import org.openfeed.InstrumentDefinition;
import org.openfeed.InstrumentReferenceResponse;
import org.openfeed.InstrumentResponse;
import org.openfeed.LoginResponse;
import org.openfeed.LogoutResponse;
import org.openfeed.MarketSnapshot;
import org.openfeed.MarketStatus;
import org.openfeed.MarketUpdate;
import org.openfeed.Ohlc;
import org.openfeed.Result;
import org.openfeed.SubscriptionResponse;
import org.openfeed.Trade;
import org.openfeed.TradeCancel;
import org.openfeed.TradeCorrection;
import org.openfeed.Trades;
import org.openfeed.Trades.Entry;
import org.openfeed.VolumeAtPrice;
import org.openfeed.client.MessageStats.StatType;
import org.openfeed.client.api.*;
import org.openfeed.client.websocket.OpenfeedClientConfigImpl;
import org.openfeed.client.websocket.PbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelPromise;

public class OpenfeedClientHandlerImpl implements OpenfeedClientHandler {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientHandler.class);
    private OpenfeedClientConfig config;
    private InstrumentCache instrumentCache;
    private ConnectionStats connectionStats;
    private int awaitingNumDefinitions;
    private ChannelPromise instrumentDownloadPromise;
    private int numDefinitions;

    public OpenfeedClientHandlerImpl(OpenfeedClientConfig config, InstrumentCache instrumentCache,
            ConnectionStats stats) {
        this.config = config;
        this.instrumentCache = instrumentCache;
        this.connectionStats = stats;
    }

    @Override
    public void onEvent(OpenfeedClient client, OpenfeedEvent event) {
        log.info("{}: Event: {} - {}", config.getClientId(), event.getType(), event.getMessage());
        switch (event.getType()) {
        case Connected:
            if (config.getStatsDisplaySeconds() > 0) {
                client.scheduleAtFixedRate(() -> {
                    log.info("{}: connected: {} {}", config.getClientId(), client.isConnected(),
                            getConnectionStats());
                }, 5, config.getStatsDisplaySeconds(), TimeUnit.SECONDS);
            }
            break;
        case Disconnected:
            connectionStats.clear();
            break;
        case Login:
            break;
        default:
            break;
        }
    }

    public OpenfeedClientHandlerImpl(OpenfeedClientConfigImpl config, InstrumentCache instrumentCache) {
        this(config, instrumentCache, new ConnectionStats());
    }

    @Override
    public void onLoginResponse(LoginResponse loginResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(loginResponse));
    }

    @Override
    public void onLogoutResponse(LogoutResponse logoutResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(logoutResponse));
    }

    @Override
    public void onInstrumentResponse(InstrumentResponse instrumentResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(instrumentResponse));
        awaitingNumDefinitions = instrumentResponse.getNumberOfDefinitions();
        numDefinitions = 0;
    }

    @Override
    public void onInstrumentReferenceResponse(InstrumentReferenceResponse instrumentReferenceResponse) {
        log.debug("{}: < {}", config.getClientId(), PbUtil.toJson(instrumentReferenceResponse));
        log.info("{}/{}/{}  ofExc: {} ddf: {} ddfExc: {} ddfBaseCode: {}", instrumentReferenceResponse.getSymbol(),instrumentReferenceResponse.getChannelId(),instrumentReferenceResponse.getMarketId(),
                instrumentReferenceResponse.getExchange(),instrumentReferenceResponse.getDdfSymbol(),instrumentReferenceResponse.getDdfExchange(),
                instrumentReferenceResponse.getDdfBaseCode()
                );
    }

    @Override
    public void onSubscriptionResponse(SubscriptionResponse subscriptionResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(subscriptionResponse));
        if (subscriptionResponse.getStatus().getResult() != Result.SUCCESS) {
            log.error("{}: Subscription Failed {}", config.getClientId(), PbUtil.toJson(subscriptionResponse));
        }
    }

    @Override
    public void onMarketStatus(MarketStatus marketStatus) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(marketStatus));
    }

    @Override
    public void onHeartBeat(HeartBeat hb) {
        if (config.isLogHeartBeat()) {
            log.info("{}: {} < {}", config.getClientId(), hb.getExchange() ? "Exchange" : "", PbUtil.toJson(hb));
        }
    }

    @Override
    public void onInstrumentDefinition(InstrumentDefinition definition) {
        if (config.isLogInstrument()) {
            log.info("< {}", PbUtil.toJson(definition));
        }
        connectionStats.getMessageStats().incrInstruments();
        this.instrumentCache.addInstrument(definition);
        this.numDefinitions++;
        if (this.awaitingNumDefinitions == numDefinitions) {
            log.info("Finished instrument download of {} instruments", this.awaitingNumDefinitions);
            this.awaitingNumDefinitions = this.numDefinitions = 0;
            if (instrumentDownloadPromise != null) {
                instrumentDownloadPromise.setSuccess();
            }
        }
        updateExchangeStats(definition.getMarketId(), StatType.instrument);
    }

    @Override
    public void onMarketSnapshot(MarketSnapshot snapshot) {
        if (config.isLogSnapshot()) {
            log.info("{}: < {}", config.getClientId(), PbUtil.toJson(snapshot));
        }
        connectionStats.getMessageStats().incrSnapshots();
        updateExchangeStats(snapshot.getMarketId(), StatType.snapshot);
    }

    @Override
    public void onMarketUpdate(MarketUpdate update) {
        connectionStats.getMessageStats().incrUpdates();
        updateExchangeStats(update.getMarketId(), StatType.update);
        //
        InstrumentDefinition definition = instrumentCache.getInstrument(update.getMarketId());
        // Symbol is optional
        String symbol = null;
        if (update.getSymbol() != null) {
            symbol = update.getSymbol();
        } else {
            symbol = definition.getSymbol();
        }
        if (config.isLogUpdate()) {
            log.info("{}: {}: < {}", config.getClientId(), symbol, PbUtil.toJson(update));
        }
        switch (update.getDataCase()) {
        case BBO:
            BestBidOffer bbo = update.getBbo();
            if (bbo.getRegional()) {
                // Regional/Participant Quote
                connectionStats.getMessageStats().incrBbo();
                updateExchangeStats(update.getMarketId(), StatType.bbo);
            } else {
                // NBBO for Equities
                connectionStats.getMessageStats().incrNBbo();
                updateExchangeStats(update.getMarketId(), StatType.nbbo);
            }
            if (config.isLogBbo()) {
                log.info("{}: {}: < {}", config.getClientId(), symbol, PbUtil.toJson(update));
            }
            break;
        case CAPITALDISTRIBUTIONS:
            break;
        case CLEARBOOK:
            break;
        case CLOSE:
            break;
        case DATA_NOT_SET:
            break;
        case DEPTHORDER:
            break;
        case DEPTHPRICELEVEL:
            break;
        case DIVIDENDSINCOMEDISTRIBUTIONS:
            break;
        case HIGH:
            break;
        case INDEX:
            break;
        case INSTRUMENTSTATUS:
            break;
        case LAST:
            break;
        case LOW:
            break;
        case MARKETSUMMARY:
            break;
        case MONETARYVALUE:
            break;
        case NETASSETVALUE:
            break;
        case NEWS:
            break;
        case NUMBEROFTRADES:
            break;
        case OPEN:
            break;
        case OPENINTEREST:
            break;
        case PREVCLOSE:
            break;
        case SETTLEMENT:
            break;
        case SHARESOUTSTANDING:
            break;
        case TRADES:
            connectionStats.getMessageStats().incrTrades();
            updateExchangeStats(update.getMarketId(), StatType.trade);
            Trades trades = update.getTrades();
            for (Entry te : trades.getTradesList()) {
                switch (te.getDataCase()) {
                case TRADE:
                    Trade trade = te.getTrade();
                    String tradeId = trade.getTradeId().toStringUtf8();
                    if (config.isLogTrade()) {
                        log.info("{}: {}/{}/{}: Trade tradeId: {}  < {}", config.getClientId(), symbol,
                                update.getMarketId(), definition.getChannel(), tradeId, PbUtil.toJson(update));
                    }
                    break;
                case TRADECANCEL:
                    TradeCancel cancel = te.getTradeCancel();
                    tradeId = cancel.getTradeId().toStringUtf8();
                    if (config.isLogTradeCancel()) {
                        log.info("{}: {}/{}/{}: Cancel tradeId: {} < {}", config.getClientId(), symbol,
                                update.getMarketId(), definition.getChannel(), tradeId, PbUtil.toJson(update));
                    }
                    break;
                case TRADECORRECTION:
                    TradeCorrection correction = te.getTradeCorrection();
                    tradeId = correction.getTradeId().toStringUtf8();
                    if (config.isLogTradeCorrection()) {
                        log.info("{}: {}/{}/{}: Correction tradeId: {} < {}", config.getClientId(), symbol,
                                update.getMarketId(), definition.getChannel(), tradeId, PbUtil.toJson(update));
                    }
                    break;
                default:
                case DATA_NOT_SET:
                    break;
                }
            }
            break;
        case VOLUME:
            break;
        case VWAP:
            break;
        case YEARHIGH:
            break;
        case YEARLOW:
            break;
        default:
            break;
        }
    }

    @Override
    public void onVolumeAtPrice(VolumeAtPrice cumulativeVolume) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(cumulativeVolume));
    }

    @Override
    public void onOhlc(Ohlc ohlc) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(ohlc));
    }

    private long getNowNs() {
        Instant now = Instant.now();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    private void updateExchangeStats(long marketId, MessageStats.StatType type) {
        InstrumentDefinition def = instrumentCache.getInstrument(marketId);
        if (def != null) {
            MessageStats stats = connectionStats.getExchangeMessageStats(def.getExchangeCode());
            switch (type) {
            case instrument:
                stats.incrInstruments();
                break;
            case bbo:
                stats.incrBbo();
                break;
            case nbbo:
                stats.incrNBbo();
                break;
            case snapshot:
                stats.incrSnapshots();
                break;
            case trade:
                stats.incrTrades();
                break;
            case update:
                stats.incrUpdates();
                break;
            default:
                break;
            }
        }
    }

    @Override
    public void setInstrumentPromise(ChannelPromise promise) {
        this.instrumentDownloadPromise = promise;
    }

    @Override
    public ConnectionStats getConnectionStats() {
        return this.connectionStats;
    }
}
