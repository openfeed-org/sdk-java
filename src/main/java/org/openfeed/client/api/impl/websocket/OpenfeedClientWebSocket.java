package org.openfeed.client.api.impl.websocket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.openfeed.*;
import org.openfeed.SubscriptionRequest.Request.Builder;
import org.openfeed.client.api.*;
import org.openfeed.client.api.OpenfeedEvent.EventType;
import org.openfeed.client.api.impl.PbUtil;
import org.openfeed.client.api.impl.Subscription;
import org.openfeed.client.api.impl.SubscriptionManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OpenfeedClientWebSocket implements OpenfeedClient, Runnable {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientWebSocket.class);
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static final int CONNECT_TIMEOUT_MSEC = 3000;
    private static final long LOGIN_WAIT_SEC = 15;
    private static final int BUF_SIZE_ENCODE = 1 * 1024;
    private static final int RCV_BUF_SIZE = 10 * (1024 * 1024);
    private final OpenfeedClientConfig config;
    private Bootstrap clientBookstrap;
    private EventLoopGroup clientEventLoopGroup;
    private OpenfeedWebSocketHandler webSocketHandler;
    private URI uri;
    private Channel channel;
    private ChannelPromise loginFuture;
    private ChannelPromise logoutFuture;
    // Session State
    private long correlationId = 1;
    private String token;
    private SubscriptionManagerImpl subscriptionManager = new SubscriptionManagerImpl();
    private ByteArrayOutputStream encodeBuf = new ByteArrayOutputStream(BUF_SIZE_ENCODE);
    private final OpenfeedClientEventHandler eventHandler;
    private final OpenfeedClientHandler clientHandler;
    //
    private AtomicBoolean running = new AtomicBoolean(true);
    private AtomicBoolean connected = new AtomicBoolean(false);
    private AtomicBoolean reconnectInProgress = new AtomicBoolean(false);
    private int numSuccessLogins = 0;

    public OpenfeedClientWebSocket(OpenfeedClientConfig config, OpenfeedClientEventHandler eventHandler, OpenfeedClientHandler clientHandler) {
        this.config = config;
        this.eventHandler = eventHandler;
        this.clientHandler = clientHandler;
    }

    @Override
    public void connectAndLogin() {
        log.info("{}: Starting Openfeed Client, user: {}", config.getClientId(), config.getUserName());
        init();
        attemptConnectAndLogin();
        // Start re-connection task
        new Thread(this).start();
    }

    @Override
    public long getCorrelationId() {
        return this.correlationId;
    }

    @Override
    public void run() {
        while (running.get()) {
            if (!connected.get()) {
                log.info("{}: Attempting reconnection in {} seconds", config.getClientId(),
                        config.getReconnectDelaySec());
                try {
                    Thread.sleep(config.getReconnectDelaySec() * 1000);
                } catch (InterruptedException ignore) {
                }
                init();
                attemptConnectAndLogin();
                if (numSuccessLogins > 1 && isLoggedIn()) {
                    resubscribe();
                    reconnectInProgress.set(false);
                }
            }
            // Wait until channel closes, if connected
            awaitChannelClose();
        }
    }

    private void resubscribe() {
        log.info("{} Resubscribing for {} subscriptions", config.getClientId(),
                subscriptionManager.getSubscriptions().size());
        for (Subscription sub : subscriptionManager.getSubscriptions()) {
            SubscriptionRequest subReq = sub.getRequest();
            // Use new correlationId
            subReq = subReq.toBuilder().setToken(this.token).build();
            // Save new request on Subscription
            sub.setRequest(subReq);
            OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
            send(req);
        }
    }

    private void init() {
        uri = null;
        try {
            uri = new URI("ws://" + config.getHost() + ":" + config.getPort() + "/ws");
        } catch (URISyntaxException ex) {
            log.error("{}: Invalid URL err: {}", config.getClientId(), ex.getMessage());
        }
        log.info("{}: Initializing connection to: {}", config.getClientId(), uri);
        // Connect with V13 (RFC 6455 aka HyBi-17).
        webSocketHandler = new OpenfeedWebSocketHandler(config, this, this.subscriptionManager,clientHandler, WebSocketClientHandshakerFactory
                .newHandshaker(uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders()));
        boolean epoll = OS.indexOf("linux") >= 0 ? true : false;

        // Ensure previous event loop was shutdown
        shutdown();
        // Configure the event loop
        if (epoll) {
            clientEventLoopGroup = new EpollEventLoopGroup();
        } else {
            clientEventLoopGroup = new NioEventLoopGroup();
        }
        log.info("{}: Using EventLoop: {}", config.getClientId(), clientEventLoopGroup.getClass());
        try {
            clientBookstrap = new Bootstrap();
            clientBookstrap.group(clientEventLoopGroup);
            if (epoll) {
                clientBookstrap.channel(EpollSocketChannel.class);
            } else {
                clientBookstrap.channel(NioSocketChannel.class);
            }
            clientBookstrap.option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MSEC)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    /*
                     * The default value is set by the /proc/sys/net/core/rmem_default file, and the
                     * maximum allowed value is set by the /proc/sys/net/core/rmem_max file.
                     */
                    .option(ChannelOption.SO_RCVBUF, RCV_BUF_SIZE)
                    //
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpClientCodec());
                            p.addLast(new HttpObjectAggregator(8192));
                            p.addLast(webSocketHandler);
                        }
                    });
        } catch (Exception e) {
            log.error("{} Initialization error: {}", config.getClientId(), e.getMessage());
            throw new RuntimeException(config.getClientId() + ": Could not initialize environment", e);
        }
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long delay, long interval, TimeUnit timeUnit) {
        channel.eventLoop().scheduleAtFixedRate(task, delay, interval, timeUnit);
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit timeUnit) {
        channel.eventLoop().schedule(task, delay, timeUnit);
    }

    private void attemptConnectAndLogin() {
        try {
            log.info("{}: Starting connection to: {}", config.getClientId(), uri);
            // Connect
            ChannelFuture connnectFuture = clientBookstrap.connect(config.getHost(), config.getPort()).sync();
            this.channel = connnectFuture.channel();
            // Wait for connect
            webSocketHandler.handshakeFuture().sync();
            this.connected.set(true);
            if(eventHandler != null) {
                eventHandler.onEvent(this, new OpenfeedEvent(EventType.Connected, "Connected to: " + uri));
            }
            // login
            login();

            log.info("{}: Successfully connected to: {} from: {}", config.getClientId(), uri,channel.localAddress());
        } catch (Exception e) {
            log.error("{}: Could not connect to uri {} err: {}", config.getClientId(), uri, e.getMessage());
            reconnectOrShutdown(config.isReconnect() ? false : true);
        }
    }

    private void reconnectOrShutdown(boolean shutdown) {
        closeConnection();
        if (!config.isReconnect() || shutdown) {
            if(!running.get()) {
                // Already shutdown
                return;
            }
            this.running.set(false);
            log.warn("{}: Closing and shutting down.", config.getClientId());
            shutdown();
        } else {
            log.info("{}: re-connecting in: {} seconds", config.getClientId(), config.getReconnectDelaySec());
            reconnectInProgress.set(true);
        }
    }

    private void closeConnection() {
        if (this.channel != null && this.channel.isActive()) {
            this.channel.close();
        }
        if(eventHandler != null) {
            eventHandler.onEvent(this, new OpenfeedEvent(EventType.Disconnected, "Disconnected from: " + uri));
        }
        this.connected.set(false);
        this.token = null;
    }

    private void shutdown() {
        if (clientEventLoopGroup != null && !clientEventLoopGroup.isShutdown()) {
            log.info("{}: Shutting down event loop", config.getClientId());
            clientEventLoopGroup.shutdownGracefully();
        }
    }

    private void awaitChannelClose() {
        if (this.channel != null && channel.isActive()) {
            try {
                this.channel.closeFuture().sync();
                log.info("{}: Channel Closed", config.getClientId());
                this.channel = null;
                closeConnection();
                // For re-subscribe mark subscriptions as unsubsribed
                subscriptionManager.setAllSubscriptionsUnsubcribed();
            } catch (InterruptedException e) {
                log.error("{}: Channel Close Issue: {}", config.getClientId(), e.getMessage());
            }
        }
    }

    @Override
    public void disconnect() {
        this.connected.set(false);
        this.token = null;
        if(isLoggedIn()) {
            logout();
        }
        reconnectOrShutdown(config.isReconnect() ? false : true);
    }



    private void login() {
        LoginRequest request = LoginRequest.newBuilder().setCorrelationId(correlationId++)
                .setUsername(config.getUserName()).setPassword(config.getPassword()).build();
        OpenfeedGatewayRequest ofreq = request().setLoginRequest(request).build();
        send(ofreq);
        this.loginFuture = this.channel.newPromise();
        try {
            boolean ret = this.loginFuture.await(LOGIN_WAIT_SEC, TimeUnit.SECONDS);
            if (!ret) {
                log.error("{}: Login timeout for user: ", config.getClientId(), request.getUsername());
            }
            else {
                numSuccessLogins++;
                if(eventHandler != null) {
                    eventHandler.onEvent(this, new OpenfeedEvent(EventType.Login, "Logged In"));
                }
            }
        } catch (InterruptedException e) {
            log.error("{}: Login Timeout err: {}", config.getClientId(), e.getMessage());
        }
    }

    private void logRequest(OpenfeedGatewayRequest ofreq) {
        log.info("{} > {}", config.getClientId(), PbUtil.toJson(ofreq));
    }

    @Override
    public void logout() {
        LogoutRequest request = LogoutRequest.newBuilder().setCorrelationId(correlationId++).setToken(this.token)
                .build();
        OpenfeedGatewayRequest ofreq = request().setLogoutRequest(request).build();
        send(ofreq);
        this.logoutFuture = this.channel.newPromise();
        try {
            boolean ret = this.logoutFuture.await(LOGIN_WAIT_SEC, TimeUnit.SECONDS);
            if (!ret) {
                log.error("Logout Timeout");
                throw new RuntimeException("Logout timeout");
            }
        } catch (InterruptedException e) {
            log.error("Logout Timeout err: {}", e.getMessage());
            throw new RuntimeException("Logout timeout");
        }
    }

    public void send(OpenfeedGatewayRequest req) {
        if(!isConnected()) {
            return;
        }
        logRequest(req);
        if (config.getWireProtocol() == OpenfeedClientConfig.WireProtocol.JSON) {
            String data = PbUtil.toJson(req);
            this.channel.writeAndFlush(new TextWebSocketFrame(data));
        } else {
            // Binary
            ByteBuf outBuf = ByteBufAllocator.DEFAULT.buffer();
            try {
                this.encodeBuf.reset();
                req.writeTo(this.encodeBuf);
                outBuf.writeBytes(encodeBuf.toByteArray());
                BinaryWebSocketFrame frame = new BinaryWebSocketFrame(outBuf);
                this.channel.writeAndFlush(frame);
            } catch (IOException e) {
                log.error("{}: Send error: {}", config.getClientId(), e.getMessage());
            }
        }
    }

    private OpenfeedGatewayRequest.Builder request() {
        return OpenfeedGatewayRequest.newBuilder();
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void completeLogin(boolean success,String error) {
        if (success) {
            this.loginFuture.setSuccess();
        } else {
            this.loginFuture.setFailure(new RuntimeException(error));
        }
    }

    public boolean isLoggedIn() {
        return connected.get() && (token != null && token.length() > 0);
    }

    public void completeLogout(boolean success) {
        if (success) {
            this.logoutFuture.setSuccess();
            this.token = null;
        } else {
            this.logoutFuture.setFailure(null);
        }
    }

    @Override
    public void instrument(String... symbols) {
        if (!isLoggedIn()) {
            return;
        }
        for (String s : symbols) {
            InstrumentRequest request = InstrumentRequest.newBuilder().setCorrelationId(correlationId++).setSymbol(s)
                    .setToken(token).build();
            OpenfeedGatewayRequest req = request().setInstrumentRequest(request).build();
            send(req);
        }
    }

    @Override
    public void instrumentMarketId(long... marketIds) {
        if (!isLoggedIn()) {
            return;
        }
        for (long id : marketIds) {
            InstrumentRequest request = InstrumentRequest.newBuilder().setCorrelationId(correlationId++).setToken(token)
                    .setMarketId(id).build();
            OpenfeedGatewayRequest req = request().setInstrumentRequest(request).build();
            send(req);
        }
    }

    @Override
    public ChannelPromise instrumentChannel(int channelId) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        InstrumentRequest request = InstrumentRequest.newBuilder().setCorrelationId(correlationId++).setToken(token)
                .setChannelId(channelId).build();
        OpenfeedGatewayRequest req = request().setInstrumentRequest(request).build();
        send(req);
        return channel.newPromise();
    }

    @Override
    public ChannelPromise instrumentExchange(String exchange) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        InstrumentRequest request = InstrumentRequest.newBuilder().setCorrelationId(correlationId++).setToken(token)
                .setExchange(exchange).build();
        OpenfeedGatewayRequest req = request().setInstrumentRequest(request).build();
        send(req);
        return channel.newPromise();
    }

    @Override
    public void instrumentReference(String... symbols) {
        if (!isLoggedIn()) {
            return;
        }
        for (String s : symbols) {
            log.info("{}: instrumentRef: {}", config.getClientId(), s);
            InstrumentReferenceRequest request = InstrumentReferenceRequest.newBuilder()
                    .setCorrelationId(correlationId++).setSymbol(s).setToken(token).build();
            OpenfeedGatewayRequest req = request().setInstrumentReferenceRequest(request).build();
            send(req);
        }
    }

    @Override
    public void instrumentReferenceMarketId(long... marketIds) {
        if (!isLoggedIn()) {
            return;
        }
        for (long id : marketIds) {
            log.info("{}: instrumentRef: {}", config.getClientId(), id);
            InstrumentReferenceRequest request = InstrumentReferenceRequest.newBuilder()
                    .setCorrelationId(correlationId++).setMarketId(id).setToken(token).build();
            OpenfeedGatewayRequest req = request().setInstrumentReferenceRequest(request).build();
            send(req);
        }
    }

    @Override
    public ChannelPromise instrumentReferenceChannel(int channelId) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        log.info("{}: instrumentRef Channel: {}", config.getClientId(), channelId);
        InstrumentReferenceRequest request = InstrumentReferenceRequest.newBuilder().setCorrelationId(correlationId++)
                .setChannelId(channelId).setToken(token).build();
        OpenfeedGatewayRequest req = request().setInstrumentReferenceRequest(request).build();
        send(req);
        return channel.newPromise();
    }

    @Override
    public void exchangeRequest() {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        log.info("{}: ExchangeReq : {}", config.getClientId());
        ExchangeRequest request = ExchangeRequest.newBuilder().setCorrelationId(correlationId++).
                setToken(token).build();
        OpenfeedGatewayRequest req = request().setExchangeRequest(request).build();
        send(req);
    }

    @Override
    public ChannelPromise instrumentReferenceExchange(String exchange) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        log.info("{}: instrumentRef Exchange: {}", config.getClientId(), exchange);
        InstrumentReferenceRequest request = InstrumentReferenceRequest.newBuilder().setCorrelationId(correlationId++)
                .setExchange(exchange).setToken(token).build();
        OpenfeedGatewayRequest req = request().setInstrumentReferenceRequest(request).build();
        send(req);
        return channel.newPromise();
    }

    @Override
    public String subscribe(Service service, SubscriptionType subscriptionType, String[] symbols) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<String> syms = new HashSet<>();
        syms.addAll(Arrays.asList(symbols));

        SubscriptionType subType = subscriptionType != null ? subscriptionType : SubscriptionType.QUOTE;
        log.info("{}: Subscribe Symbol: {}", config.getClientId(), Arrays.asList(syms.toArray()));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (String symbol : syms) {
//            if(this.subscriptionManager.isSubscribed(symbol) ) {
//                log.warn("Already subscribed to: {}",symbol);
//                continue;
//            }
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            // Subscription Type
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        if(request.getRequestsCount() == 0) {
            // already subscribed or no subscription items,  bail
            return null;
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "symbol");
        subscriptionManager.addSubscription(subscriptionId, subReq, symbols);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribe(Service service, SubscriptionType[] subscriptionTypes, String[] symbols) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<String> syms = new HashSet<>();
        syms.addAll(Arrays.asList(symbols));

        log.info("{}: Subscribe Symbol: {}", config.getClientId(), Arrays.asList(syms.toArray()));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (String symbol : syms) {
            // TODO
//            if(this.subscriptionManager.isSubscribed(symbol) ) {
//                log.warn("Already subscribed to: {}",symbol);
//                continue;
//            }
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            // Subscription Types
            if(subscriptionTypes != null && subscriptionTypes.length > 0) {
                Arrays.stream(subscriptionTypes).forEach( type -> subReq.addSubscriptionType(type));
            }
            else {
                subReq.addSubscriptionType(SubscriptionType.QUOTE);
            }
            request.addRequests(subReq);
        }
        if(request.getRequestsCount() == 0) {
            // already subscribed or no subscription items,  bail
            return null;
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "symbol");
        subscriptionManager.addSubscription(subscriptionId, subReq, symbols);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }



    private String createSubscriptionId(String userName, Service service, long correlationId, String type) {
        return userName + ":" + service.getNumber() + ":" + correlationId;
    }

    @Override
    public String subscribe(Service service, SubscriptionType subscriptionType, long[] marketIds) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<Long> ids = new HashSet<Long>();
        Arrays.stream(marketIds).forEach( id -> ids.add(id));

        SubscriptionType subType = subscriptionType != null ? subscriptionType : SubscriptionType.QUOTE;
        log.info("{}: Subscribe Openfeed Id: {}", config.getClientId(), Arrays.asList(ids));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (Long id : ids) {
            // TODO
//            if(this.subscriptionManager.isSubscribed(id) ) {
//                log.warn("Already subscribed to: {}",id);
//                continue;
//            }
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "marketId");
        subscriptionManager.addSubscription(subscriptionId, subReq, marketIds);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribe(Service service, SubscriptionType[] subscriptionTypes, long[] marketIds) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<Long> ids = new HashSet<Long>();
        Arrays.stream(marketIds).forEach( id -> ids.add(id));

        log.info("{}: Subscribe Openfeed Id: {}", config.getClientId(), Arrays.asList(ids));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (Long id : ids) {
//            if(this.subscriptionManager.isSubscribed(id) ) {
//                log.warn("Already subscribed to: {}",id);
//                continue;
//            }
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            if(subscriptionTypes != null && subscriptionTypes.length > 0) {
                Arrays.stream(subscriptionTypes).forEach( type -> subReq.addSubscriptionType(type));
            }
            else {
                subReq.addSubscriptionType(SubscriptionType.QUOTE);
            }
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "marketId");
        subscriptionManager.addSubscription(subscriptionId, subReq, marketIds);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeExchange(Service service, SubscriptionType subscriptionType, String[] exchanges) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<String> exchs = new HashSet<>();
        exchs.addAll(Arrays.asList(exchanges));

        SubscriptionType subType = subscriptionType != null ? subscriptionType : SubscriptionType.QUOTE;
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (String exchange : exchs) {
//            if(this.subscriptionManager.isSubscribedExchange(exchange) ) {
//                log.warn("Already subscribed to: {}",exchange);
//                continue;
//            }
            log.info("{}: Subscribe Exchange: {}", config.getClientId(), exchange);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            // Subscription Types
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "exchange");
        subscriptionManager.addSubscriptionExchange(subscriptionId, subReq, exchanges);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeChannel(Service service, SubscriptionType subscriptionType, int[] channelIds) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<Integer> ids = new HashSet<>();
        Arrays.stream(channelIds).forEach( id -> ids.add(id));

        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (Integer id : ids) {
            if(this.subscriptionManager.isSubscribedChannel(id) ) {
                log.warn("Already subscribed to: {}",id);
                continue;
            }
            log.info("{}: Subscribe Channel: {}", config.getClientId(), id);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setChannelId(id);
            if (subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, correlationId, "channel");
        subscriptionManager.addSubscriptionChannel(subscriptionId, subReq, channelIds);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeSnapshot(String[] symbols, int intervalSec) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<String> syms = new HashSet<>();
        Arrays.stream(symbols).forEach( s -> syms.add(s));

        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(Service.REAL_TIME_SNAPSHOT);
        for (String symbol : syms) {
            log.info("{}: Subscribe Snapshot: {}", config.getClientId(), symbol);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol)
                    .setSnapshotIntervalSeconds(intervalSec);
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), Service.REAL_TIME_SNAPSHOT, correlationId,
                "snapshot");
        subscriptionManager.addSubscription(subscriptionId, subReq, symbols);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public void unSubscribe(Service service, String[] symbols) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Symbols: {}", config.getClientId(), Arrays.asList(symbols));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String symbol : symbols) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscription(symbols);
    }

    @Override
    public void unSubscribe(Service service,SubscriptionType subscriptionType, String[] symbols) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Symbols: {} subType: {}", config.getClientId(), Arrays.asList(symbols),subscriptionType);
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String symbol : symbols) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            if(subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscription(symbols);
    }

    @Override
    public void unSubscribe(Service service, long[] marketIds) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Ids: {}", config.getClientId(), Arrays.asList(marketIds));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (Long id : marketIds) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscription(marketIds);
    }

    @Override
    public void unSubscribe(Service service,SubscriptionType subscriptionType, long[] marketIds) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Ids: {}", config.getClientId(), Arrays.asList(marketIds));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (Long id : marketIds) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            if(subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscription(marketIds);
    }

    @Override
    public void unSubscribeExchange(Service service, String[] exchanges) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Exchanges: {}", config.getClientId(), Arrays.asList(exchanges));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String exchange : exchanges) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscriptionExchange(exchanges);
    }

    @Override
    public void unSubscribeExchange(Service service,SubscriptionType subscriptionType, String[] exchanges) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Exchanges: {}", config.getClientId(), Arrays.asList(exchanges));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String exchange : exchanges) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            if(subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscriptionExchange(exchanges);
    }

    @Override
    public void unSubscribeChannel(Service service, int[] channelIds) {
        if (!isLoggedIn()) {
            return;
        }
        log.info("{}: Un Subscribe Channel: {}", config.getClientId(), Arrays.asList(channelIds));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (int channelId : channelIds) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setChannelId(channelId);
            request.addRequests(subReq);
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        subscriptionManager.removeSubscriptionChannel(channelIds);
    }

    @Override
    public boolean isConnected() {
        return this.connected.get();
    }

    @Override
    public boolean isReConnect() {
        return numSuccessLogins > 1 && this.reconnectInProgress.get();
    }

    public void setConnected(boolean b) {
        this.connected.set(b);
    }

    @Override
    public String getToken() {
        return this.token;
    }

    @Override
    public Collection<Subscription> getSubscriptions() {
        Collection<Subscription> subscriptions = subscriptionManager.getSubscriptions();
        return subscriptions;
    }

    @Override
    public Subscription getSubscription(String subscriptionId) {
        return this.subscriptionManager.getSubscriptionId(subscriptionId);
    }
}
