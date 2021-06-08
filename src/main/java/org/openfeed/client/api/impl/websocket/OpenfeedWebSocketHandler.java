package org.openfeed.client.api.impl.websocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.openfeed.*;
import org.openfeed.client.api.OpenfeedClientConfig;
import org.openfeed.client.api.OpenfeedClientHandler;
import org.openfeed.client.api.impl.PbUtil;
import org.openfeed.client.api.impl.SubscriptionManagerImpl;
import org.openfeed.client.api.impl.WireStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class OpenfeedWebSocketHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedWebSocketHandler.class);

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private OpenfeedClientWebSocket client;
    private OpenfeedClientConfig config;
    private final SubscriptionManagerImpl subscriptionManager;
    private OpenfeedClientHandler clientHandler;
    private WireStats stats;

    public OpenfeedWebSocketHandler(OpenfeedClientConfig config, OpenfeedClientWebSocket client, SubscriptionManagerImpl subscriptionManager,
                                    OpenfeedClientHandler clientHandler, WebSocketClientHandshaker handshaker) {
        this.config = config;
        this.subscriptionManager = subscriptionManager;
        this.clientHandler = clientHandler;
        this.handshaker = handshaker;
        this.client = client;
    }

    public WireStats getStats() { return this.stats; }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
        // Dump config
        ChannelConfig config = ctx.channel().config();
        SocketChannelConfig socketConfig = (SocketChannelConfig) config;
        Map<ChannelOption<?>, Object> options = socketConfig.getOptions();
        StringBuilder sb = new StringBuilder(ctx.channel().remoteAddress() + ": Options\n");
        for (Entry<ChannelOption<?>, Object> opt : options.entrySet()) {
            sb.append(opt.getKey() + "=" + opt.getValue() + ",");
        }
        log.info("{}", sb.toString());
        if (this.config.isWireStats()) {
            this.stats = new WireStats();
        }
    }

    private void logStats() {
        log.info("{}",this.stats);
        this.stats.reset();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
        if(this.config.isWireStats()) {
            // Track some wire metrics
            ctx.channel().eventLoop().scheduleAtFixedRate(this::logStats,1,1, TimeUnit.SECONDS);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("WebSocket Client disconnected. {}", ctx.channel().localAddress());
        this.client.disconnect();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                log.info("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                log.error("WebSocket Client failed to connect");
                handshakeFuture.setFailure(e);
            }
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException("Unexpected FullHttpResponse (getStatus=" + response.status() + ", content="
                    + response.content().toString(CharsetUtil.UTF_8) + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            OpenfeedGatewayMessage ofgm = decodeJson(textFrame.text());
            handleResponse(ofgm);
        } else if (frame instanceof BinaryWebSocketFrame) {
            try {
                BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                final byte[] array;
                final int offset;
                ByteBuf binBuf = frame.content();
                final int length = binBuf.readableBytes();
                if(this.config.isWireStats()) {
                    this.stats.update(length);
                }

                if (binBuf.hasArray()) {
                    array = binBuf.array();
                    offset = binBuf.arrayOffset() + binBuf.readerIndex();
                } else {
                    array = ByteBufUtil.getBytes(binBuf, binBuf.readerIndex(), length, false);
                    offset = 0;
                }
                ByteArrayInputStream bis = new ByteArrayInputStream(array);
                OpenfeedGatewayMessage rsp = OpenfeedGatewayMessage.parseFrom(bis);
                handleResponse(rsp);
            } catch (Exception e) {
                log.error("{}: Could not process message: ", ctx.channel().remoteAddress(), e);
            }
        } else if (frame instanceof PongWebSocketFrame) {
            log.info("WebSocket Client received pong");
        } else if (frame instanceof CloseWebSocketFrame) {
            log.info("WebSocket Client received closing");
            ch.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("ExceptionCaught from: {}", ctx.channel().remoteAddress(), cause);
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
        client.disconnect();
    }

    private OpenfeedGatewayMessage decodeJson(String data) {
        try {
            OpenfeedGatewayMessage.Builder rsp = OpenfeedGatewayMessage.newBuilder();
            PbUtil.decode(data, rsp);
            return rsp.build();
        } catch (Exception e) {
            throw new RuntimeException("decode exception, data: " + data, e);
        }
    }

    void handleResponse(OpenfeedGatewayMessage ofgm) {
        if (ofgm == null) {
            log.warn("Empty Message received.");
            return;
        }
        if (config.isLogAll()) {
            log.info("< {}", PbUtil.toJson(ofgm));
        }
        switch (ofgm.getDataCase()) {
            case LOGINRESPONSE:
                LoginResponse loginResponse = ofgm.getLoginResponse();
                if (loginResponse.getStatus().getResult() == Result.SUCCESS) {
                    log.debug("{}: Login successful: token {}", config.getClientId(), PbUtil.toJson(ofgm));
                    this.client.setToken(loginResponse.getToken());
                    this.client.completeLogin(true,null);
                } else {
                    String error = config.getClientId() + ": Login failed: " + PbUtil.toJson(ofgm);
                    log.error("{}",error);
                    this.client.completeLogin(false,error);
                }
                clientHandler.onLoginResponse(ofgm.getLoginResponse());
                break;
            case LOGOUTRESPONSE:
                LogoutResponse logout = ofgm.getLogoutResponse();
                if (logout.getStatus().getResult() == Result.SUCCESS) {
                    this.client.completeLogout(true);
                } else {
                    this.client.completeLogout(false);
                }
                clientHandler.onLogoutResponse(ofgm.getLogoutResponse());
                break;
            case INSTRUMENTRESPONSE:
                clientHandler.onInstrumentResponse(ofgm.getInstrumentResponse());
                break;
            case INSTRUMENTREFERENCERESPONSE:
                clientHandler.onInstrumentReferenceResponse(ofgm.getInstrumentReferenceResponse());
                break;
            case EXCHANGERESPONSE:
                clientHandler.onExchangeResponse(ofgm.getExchangeResponse());
                break;
            case SUBSCRIPTIONRESPONSE:
                // Update Subscription State
                this.subscriptionManager.updateSubscriptionState(ofgm.getSubscriptionResponse());
                clientHandler.onSubscriptionResponse(ofgm.getSubscriptionResponse());
                break;
            case MARKETSTATUS:
                clientHandler.onMarketStatus(ofgm.getMarketStatus());
                break;
            case HEARTBEAT:
                clientHandler.onHeartBeat(ofgm.getHeartBeat());
                break;
            case INSTRUMENTDEFINITION:
                InstrumentDefinition def = ofgm.getInstrumentDefinition();
                this.client.addMapping(def);
                clientHandler.onInstrumentDefinition(ofgm.getInstrumentDefinition());
                break;
            case MARKETSNAPSHOT:
                clientHandler.onMarketSnapshot(ofgm.getMarketSnapshot());
                break;
            case MARKETUPDATE:
                clientHandler.onMarketUpdate(ofgm.getMarketUpdate());
                break;
            case VOLUMEATPRICE:
                clientHandler.onVolumeAtPrice(ofgm.getVolumeAtPrice());
                break;
            case OHLC:
                clientHandler.onOhlc(ofgm.getOhlc());
                break;
            case INSTRUMENTACTION:
                clientHandler.onInstrumentAction(ofgm.getInstrumentAction());
                break;
            default:
            case DATA_NOT_SET:
                break;
        }

    }


}
