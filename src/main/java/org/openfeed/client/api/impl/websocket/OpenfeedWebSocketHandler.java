package org.openfeed.client.api.impl.websocket;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.openfeed.LoginResponse;
import org.openfeed.LogoutResponse;
import org.openfeed.OpenfeedGatewayMessage;
import org.openfeed.Result;
import org.openfeed.client.api.OpenfeedClientHandler;
import org.openfeed.client.api.OpenfeedClientMessageHandler;
import org.openfeed.client.api.impl.OpenfeedClientConfigImpl;
import org.openfeed.client.api.impl.PbUtil;
import org.openfeed.client.api.impl.SubscriptionManagerImpl;
import org.openfeed.client.api.impl.WireStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

public class OpenfeedWebSocketHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedWebSocketHandler.class);
    private static final int QUEUE_BATCH_SIZE = 2_000;

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    private OpenfeedClientWebSocket client;
    private OpenfeedClientConfigImpl config;
    private final SubscriptionManagerImpl subscriptionManager;
    private OpenfeedClientHandler clientHandler;
    private final OpenfeedClientMessageHandler messageHandler;
    private WireStats wireStats;
    // Off load response processing
    private final ExecutorService executorService;
    private final BlockingQueue<Dto> messageQueue = new LinkedBlockingQueue<>();
    private final Future<?> processingThreadFuture;

    public OpenfeedWebSocketHandler(OpenfeedClientConfigImpl config, OpenfeedClientWebSocket client, SubscriptionManagerImpl subscriptionManager,
                                    OpenfeedClientHandler clientHandler, WebSocketClientHandshaker handshaker, OpenfeedClientMessageHandler messageHandler) {
        this.config = config;
        this.subscriptionManager = subscriptionManager;
        this.clientHandler = clientHandler;
        this.handshaker = handshaker;
        this.client = client;
        this.messageHandler = messageHandler;
        this.executorService = Executors.newCachedThreadPool((new ThreadFactoryBuilder()).setNameFormat(config.getClientId()).build());
        // Message processing thread
        this.processingThreadFuture = this.executorService.submit(() -> processMessageQueue());
        if(config.getWireStatsDisplaySeconds() > 0) {
            this.wireStats = new WireStats();
        }
    }

    public WireStats getWireStats() {
        return this.wireStats;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    private void processMessageQueue() {
        List<Dto> items = new ArrayList<>(QUEUE_BATCH_SIZE);
        long ts = System.currentTimeMillis();
        while (true) {
            if(config.getWireStatsDisplaySeconds() > 0) {
                long t = System.currentTimeMillis();
                if ((t - ts) / 1000 > config.getWireStatsDisplaySeconds()) {
                    ts = t;
                    log.info("{}: message Q size: {}", config.getClientId(), this.messageQueue.size());
                }
            }

            items.clear();
            Dto o = null;
            try {
                o = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (o != null) {
                    items.add(o);
                    messageQueue.drainTo(items, QUEUE_BATCH_SIZE);
                }
            } catch (InterruptedException e) {
                log.warn("{}: message processing issue: {}",config.getClientId(), e.getMessage());
            }
            if (items.isEmpty()) {
                continue;
            }
            //
            items.forEach( m -> handleResponse(m.message,m.bytes));
        }
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
        log.debug("{}: options: {}",ctx,sb);

        log.info("{}: recvBufSize: {}", ctx,((SocketChannelConfig) config).getReceiveBufferSize());
    }

    private void logWireStats() {
        log.info("{}", this.wireStats);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handshaker.handshake(ctx.channel());
        if (this.config.getWireStatsDisplaySeconds() > 0) {
            // Track some wire metrics
            ctx.channel().eventLoop().scheduleAtFixedRate(this::logWireStats, 4, config.getWireStatsDisplaySeconds(), TimeUnit.SECONDS);
        }
        if(this.config.getPingSeconds() > 0) {
            log.info("{}: Sending Ping messages every {} seconds",ctx,config.getPingSeconds());
            ctx.channel().eventLoop().scheduleAtFixedRate(() -> sendPingFrame(ctx,null),10, config.getPingSeconds(), TimeUnit.SECONDS);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("WebSocket Client disconnected. {}", ctx.channel().localAddress());
        this.client.disconnect();
        this.processingThreadFuture.cancel(true);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                log.info("{}: WebSocket Client connected!", config.getClientId());
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                log.error("{}: WebSocket Client failed to connect", config.getClientId());
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
        if(frame instanceof  TextWebSocketFrame) {
            // Not used
        }
        else if(frame instanceof PingWebSocketFrame) {
            this.wireStats.incrPingsReceived();
            ByteBuf binBuf = frame.content();
            final int length = binBuf.readableBytes();
            final byte[] array = ByteBufUtil.getBytes(binBuf, binBuf.readerIndex(), length, false);
            String payload = new String(array);
            sendPongFrame(ctx,payload);
            log.debug("{}: Client received Ping: {}",ctx.channel().remoteAddress(),payload);
        }
        else if (frame instanceof BinaryWebSocketFrame) {
            try {
                ByteBuf binBuf = frame.content();
                final int length = binBuf.readableBytes();
                final byte[] array = ByteBufUtil.getBytes(binBuf, binBuf.readerIndex(), length, false);
                parseMessage(length, array);
            } catch (Exception e) {
                log.error("{}: Could not process message: ", ctx.channel().remoteAddress(), e);
            }
        } else if (frame instanceof PongWebSocketFrame) {
            this.wireStats.incrPongsReceived();
            PongWebSocketFrame pong = (PongWebSocketFrame) frame;
            ByteBuf binBuf = pong.content();
            final int length = binBuf.readableBytes();
            final byte[] array = ByteBufUtil.getBytes(binBuf, binBuf.readerIndex(), length, false);
            log.debug("{}: Client received Pong: {}",ctx.channel().remoteAddress(),new String(array));
        } else if (frame instanceof CloseWebSocketFrame) {
            log.warn("WebSocket Client received Close Frame");
            ch.close();
        }
    }

    void parseMessage(int length, byte[] array) throws InvalidProtocolBufferException {
        if (config.getProtocolVersion() == 0) {
            if (this.wireStats != null) {
                this.wireStats.update(length,0);
            }
            OpenfeedGatewayMessage rsp = OpenfeedGatewayMessage.parseFrom(array);
            messageQueue.offer(new Dto(rsp, array));
        } else {
            int msgs =0;
            final ByteBuffer byteBuffer = ByteBuffer.wrap(array);
            while (byteBuffer.remaining() > 0) {
                int msgLen = byteBuffer.getShort() & 0xFFFF;
                if (msgLen > byteBuffer.remaining()) {
                    log.error("Corrupt packet, array: {} msgLen: {} buf: {}", array.length,msgLen, byteBuffer);
                    break;
                }
                byte[] ofMsgBytes = new byte[msgLen];
                byteBuffer.get(ofMsgBytes);
                OpenfeedGatewayMessage rsp = null;
                try {
                    rsp = OpenfeedGatewayMessage.parseFrom(ofMsgBytes);
                    msgs++;
                }
                catch(Exception e) {
                    log.error("Could not decode message dataLength: {} msgNo: {} buf: {} msgLen: {} error: {}", array.length,msgs,byteBuffer,msgLen, e.getMessage());
                    break;
                }
                this.messageQueue.offer(new Dto(rsp,ofMsgBytes));
            }
            if (this.wireStats != null) {
                this.wireStats.update(length,msgs);
            }
        }
    }

    private void sendPingFrame(ChannelHandlerContext ctx,String payload) {
        try {
            StringBuilder sb = new StringBuilder(DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.now()) + ": ").append(ctx.channel().localAddress() + ": ").append(client.getToken());
            if(!Strings.isNullOrEmpty(payload)) {
                sb.append(payload);
            }
            ByteBuf outBuf  = ctx.alloc().buffer(sb.length());
            outBuf.writeBytes(sb.toString().getBytes());
            PingWebSocketFrame frame = new PingWebSocketFrame(outBuf);
            ctx.writeAndFlush(frame);
        } catch (Exception e) {
            log.error("{}: sendPing error: {}", config.getClientId(), e.getMessage());
        }
    }

    private void sendPongFrame(ChannelHandlerContext ctx,String payload) {
        try {
            ByteBuf buf = ctx.alloc().buffer(payload.length());
            buf.writeBytes(payload.getBytes());
            PongWebSocketFrame frame = new PongWebSocketFrame(buf);
            ctx.writeAndFlush(frame);
        } catch (Exception e) {
            log.error("{}: sendPong error: {}", config.getClientId(), e.getMessage());
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
        this.processingThreadFuture.cancel(true);
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

    void handleResponse(OpenfeedGatewayMessage ofgm, byte[] bytes) {
        if (ofgm == null) {
            log.warn("Empty Message received.");
            return;
        }
        if (config.isLogAll()) {
            logMsg(ofgm);
        }

        switch (ofgm.getDataCase()) {
            case LOGINRESPONSE:
                log(ofgm);
                LoginResponse loginResponse = ofgm.getLoginResponse();
                if (loginResponse.getStatus().getResult() == Result.SUCCESS) {
                    log.debug("{}: Login successful: token {}", config.getClientId(), PbUtil.toJson(ofgm));
                    this.client.setToken(loginResponse.getToken());
                    this.client.completeLogin(true, null);
                } else {
                    String error = config.getClientId() + ": Login failed: " + PbUtil.toJson(ofgm);
                    log.error("{}", error);
                    this.client.completeLogin(false, error);
                }
                if (clientHandler != null) {
                    clientHandler.onLoginResponse(ofgm.getLoginResponse());
                }
                break;
            case LOGOUTRESPONSE:
                log(ofgm);
                LogoutResponse logout = ofgm.getLogoutResponse();
                if (logout.getStatus().getResult() == Result.SUCCESS) {
                    this.client.completeLogout(true);
                } else if (logout.getStatus().getResult() == Result.DUPLICATE_LOGIN &&
                        config.isDisableClientOnDuplicateLogin()) {
                    log.error("{}: Duplicate Login, stopping client: {}", config.getClientId(), PbUtil.toJson(ofgm));
                    this.config.setReconnect(false);
                    this.client.disconnect();
                } else {
                    this.client.completeLogout(false);
                }

                if (clientHandler != null) {
                    clientHandler.onLogoutResponse(ofgm.getLogoutResponse());
                }
                break;
            case INSTRUMENTRESPONSE:
                if (clientHandler != null) {
                    clientHandler.onInstrumentResponse(ofgm.getInstrumentResponse());
                }
                break;
            case INSTRUMENTREFERENCERESPONSE:
                if (clientHandler != null) {
                    clientHandler.onInstrumentReferenceResponse(ofgm.getInstrumentReferenceResponse());
                }
                break;
            case EXCHANGERESPONSE:
                log(ofgm);
                if (clientHandler != null) {
                    clientHandler.onExchangeResponse(ofgm.getExchangeResponse());
                }
                break;
            case SUBSCRIPTIONRESPONSE:
                log(ofgm);
                // Update Subscription State
                this.subscriptionManager.updateSubscriptionState(ofgm.getSubscriptionResponse());
                if (clientHandler != null) {
                    clientHandler.onSubscriptionResponse(ofgm.getSubscriptionResponse());
                }
                break;
            case MARKETSTATUS:
                log(ofgm);
                if (clientHandler != null) {
                    clientHandler.onMarketStatus(ofgm.getMarketStatus());
                }
                break;
            case HEARTBEAT:
                if (config.isLogHeartBeat()) {
                    logMsg(ofgm);
                }
                if (clientHandler != null) {
                    clientHandler.onHeartBeat(ofgm.getHeartBeat());
                }
                break;
            case INSTRUMENTDEFINITION:
                if (clientHandler != null) {
                    clientHandler.onInstrumentDefinition(ofgm.getInstrumentDefinition());
                }
                break;
            case MARKETSNAPSHOT:
                if (clientHandler != null) {
                    clientHandler.onMarketSnapshot(ofgm.getMarketSnapshot());
                }
                break;
            case MARKETUPDATE:
                if (clientHandler != null) {
                    clientHandler.onMarketUpdate(ofgm.getMarketUpdate());
                }
                break;
            case VOLUMEATPRICE:
                if (clientHandler != null) {
                    clientHandler.onVolumeAtPrice(ofgm.getVolumeAtPrice());
                }
                break;
            case OHLC:
                if (clientHandler != null) {
                    clientHandler.onOhlc(ofgm.getOhlc());
                }
                break;
            case INSTRUMENTACTION:
                if (clientHandler != null) {
                    clientHandler.onInstrumentAction(ofgm.getInstrumentAction());
                }
                break;
            case LISTSUBSCRIPTIONSRESPONSE:
                if (clientHandler != null) {
                    clientHandler.onListSubscriptionsResponse(ofgm.getListSubscriptionsResponse());
                }
                break;
            default:
            case DATA_NOT_SET:
                break;
        }

        if (messageHandler != null) {
            messageHandler.onMessage(ofgm,bytes);
        }

    }

    private void log(OpenfeedGatewayMessage ofmsg) {
        if (config.isLogRequestResponse()) {
            logMsg(ofmsg);
        }
    }

    private void logMsg(OpenfeedGatewayMessage ofmsg) {
        log.info("{} < {}", config.getClientId(), PbUtil.toJson(ofmsg));
    }

    class Dto {
        OpenfeedGatewayMessage message;
        byte[] bytes;

        public Dto(OpenfeedGatewayMessage message, byte[] bytes) {
            this.message = message;
            this.bytes = bytes;
        }
    }

}
