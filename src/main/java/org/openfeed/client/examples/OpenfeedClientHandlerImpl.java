package org.openfeed.client.examples;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.openfeed.*;
import org.openfeed.Trades.Entry;
import org.openfeed.client.api.impl.ConnectionStats;
import org.openfeed.client.api.impl.MessageStats;
import org.openfeed.client.api.impl.MessageStats.StatType;
import org.openfeed.client.api.*;
import org.openfeed.client.api.impl.OpenfeedClientConfigImpl;
import org.openfeed.client.api.impl.PbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelPromise;

public class OpenfeedClientHandlerImpl implements OpenfeedClientHandler {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientHandler.class);
    private OpenfeedClientConfig config;
    private InstrumentCache instrumentCache;
    private ConnectionStats connectionStats;
    private MarketsManager marketsManager;
    private int awaitingNumDefinitions;
    private ChannelPromise instrumentDownloadPromise;
    private int numDefinitions;

    public OpenfeedClientHandlerImpl(OpenfeedClientConfig config, InstrumentCache instrumentCache,
                                     ConnectionStats stats, MarketsManager marketsManager) {
        this.config = config;
        this.instrumentCache = instrumentCache;
        this.connectionStats = stats;
        this.marketsManager = marketsManager;
    }

    public OpenfeedClientHandlerImpl(OpenfeedClientConfigImpl config, InstrumentCache instrumentCache, MarketsManager marketsManager) {
        this(config, instrumentCache, new ConnectionStats(), marketsManager);
    }

    @Override
    public void onLoginResponse(LoginResponse loginResponse) {
        log.info("{}: < LoginResponse {}", config.getClientId(), PbUtil.toJson(loginResponse));
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
        log.info("{}/{}/{}  ofExc: {} ddf: {} ddfExc: {} ddfBaseCode: {}", instrumentReferenceResponse.getSymbol(), instrumentReferenceResponse.getChannelId(), instrumentReferenceResponse.getMarketId(),
                instrumentReferenceResponse.getExchange(), instrumentReferenceResponse.getDdfSymbol(), instrumentReferenceResponse.getDdfExchange(),
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
        connectionStats.getMessageStats().incrHeartBeats();
    }

    @Override
    public void onInstrumentDefinition(InstrumentDefinition definition) {
        if (config.isLogInstrument()) {
            log.info("INSTRUMENT {}: < {}", config.getClientId(), PbUtil.toJson(definition));
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

        marketsManager.createMarket(definition);
    }

    @Override
    public void onMarketSnapshot(MarketSnapshot snapshot) {
        if (config.isLogSnapshot()) {
            log.info("SNAPSHOT {}: < {}", config.getClientId(), PbUtil.toJson(snapshot));
        }
        connectionStats.getMessageStats().incrSnapshots();
        updateExchangeStats(snapshot.getMarketId(), StatType.snapshot);

        Optional<MarketState> market = marketsManager.getMarket(snapshot.getMarketId());
        if (market.isPresent()) {
            market.get().apply(snapshot);
            if (config.isLogDepth()) {
                if (config.isLogDepth()) {
                    log.info("SNAPSHOT DEPTH:\n{}", market.get().getDepthPriceLevel());
                }
            }
        }
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
            log.info("UPDATE: {}: {}: < {}", config.getClientId(), symbol, PbUtil.toJson(update));
        }

        Optional<MarketState> market = marketsManager.getMarket(update.getMarketId());
        if (market.isPresent()) {
            market.get().apply(update);
            if (config.isLogDepth()) {
                log.info("{}", market.get().getDepthPriceLevel());
            }
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
                connectionStats.getMessageStats().incrDepthOrder();
                updateExchangeStats(update.getMarketId(), StatType.depth_order);
                break;
            case DEPTHPRICELEVEL:
                connectionStats.getMessageStats().incrDepthPrice();
                updateExchangeStats(update.getMarketId(), StatType.depth_price);
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
                Trades trades = update.getTrades();
                for (Entry te : trades.getTradesList()) {
                    switch (te.getDataCase()) {
                        case TRADE:
                            connectionStats.getMessageStats().incrTrades();
                            updateExchangeStats(update.getMarketId(), StatType.trade);
                            Trade trade = te.getTrade();
                            String tradeId = trade.getTradeId().toStringUtf8();
                            if (config.isLogTrade()) {
                                log.info("{}: {}/{}/{}: Trade tradeId: {}  < {}", config.getClientId(), symbol,
                                        update.getMarketId(), definition.getChannel(), tradeId, PbUtil.toJson(update));
                            }
                            break;
                        case TRADECANCEL:
                            connectionStats.getMessageStats().incrTradeCancel();
                            updateExchangeStats(update.getMarketId(), StatType.trade_cancel);
                            TradeCancel cancel = te.getTradeCancel();
                            tradeId = cancel.getTradeId().toStringUtf8();
                            if (config.isLogTradeCancel()) {
                                log.info("{}: {}/{}/{}: Cancel tradeId: {} < {}", config.getClientId(), symbol,
                                        update.getMarketId(), definition.getChannel(), tradeId, PbUtil.toJson(update));
                            }
                            break;
                        case TRADECORRECTION:
                            connectionStats.getMessageStats().incrTradeCorrection();
                            updateExchangeStats(update.getMarketId(), StatType.trade_correction);
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
        connectionStats.getMessageStats().incrOHLC();
        updateExchangeStats(ohlc.getMarketId(), StatType.ohlc);
        if (config.isLogOhlc()) {
            log.info("{}: < {}", config.getClientId(), PbUtil.toJson(ohlc));
        }
    }

    @Override
    public void onInstrumentAction(InstrumentAction instrumentAction) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(instrumentAction));
    }

    @Override
    public void onListSubscriptionsResponse(ListSubscriptionsResponse listSubscriptionsResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(listSubscriptionsResponse));
    }

    private long getNowNs() {
        Instant now = Instant.now();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    private void updateExchangeStats(long marketId, MessageStats.StatType type) {
        InstrumentDefinition def = instrumentCache.getInstrument(marketId);
        if (def != null) {
            MessageStats stats = connectionStats.getExchangeMessageStats(def.getChannel(), def.getExchangeCode());
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
                case trade_correction:
                    stats.incrTradeCorrection();
                    break;
                case trade_cancel:
                    stats.incrTradeCancel();
                    break;
                case update:
                    stats.incrUpdates();
                    break;
                case ohlc:
                    stats.incrOHLC();
                    break;
                case depth_price:
                    stats.incrDepthPrice();
                    break;
                case depth_order:
                    stats.incrDepthOrder();
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
    public void onExchangeResponse(ExchangeResponse exchangeResponse) {
        log.info("{}: < {}", config.getClientId(), PbUtil.toJson(exchangeResponse));
    }

    @Override
    public ConnectionStats getConnectionStats() {
        return this.connectionStats;
    }
}
