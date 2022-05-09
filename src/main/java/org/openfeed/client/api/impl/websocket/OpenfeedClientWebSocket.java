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
import org.agrona.collections.Long2ObjectHashMap;
import org.openfeed.*;
import org.openfeed.SubscriptionRequest.Request.Builder;
import org.openfeed.client.api.*;
import org.openfeed.client.api.OpenfeedEvent.EventType;
import org.openfeed.client.api.impl.OpenfeedClientConfigImpl;
import org.openfeed.client.api.impl.PbUtil;
import org.openfeed.client.api.impl.Subscription;
import org.openfeed.client.api.impl.SubscriptionManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OpenfeedClientWebSocket implements OpenfeedClient, Runnable {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientWebSocket.class);
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static final int CONNECT_TIMEOUT_MSEC = 3000;
    private static final long LOGIN_WAIT_SEC = 15;
    private static final int BUF_SIZE_ENCODE = 1 * 1024;
    private final OpenfeedClientConfigImpl config;
    private Bootstrap clientBootstrap;
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
    private final OpenfeedClientMessageHandler messageHandler;
    //
    private AtomicBoolean running = new AtomicBoolean(true);
    private AtomicBoolean connected = new AtomicBoolean(false);
    private AtomicBoolean reconnectInProgress = new AtomicBoolean(false);
    private int numSuccessLogins = 0;
    private Map<Long, String> marketIdToSymbol = new Long2ObjectHashMap<>();
    private final String clientVersion;

    public OpenfeedClientWebSocket(OpenfeedClientConfigImpl config, OpenfeedClientEventHandler eventHandler,
                                   OpenfeedClientHandler clientHandler) {
        this(config,eventHandler,clientHandler,null);
    }

    public OpenfeedClientWebSocket(OpenfeedClientConfigImpl config, OpenfeedClientEventHandler eventHandler,
                                   OpenfeedClientMessageHandler messageHandler) {
        this(config,eventHandler,null,messageHandler);
    }

    public OpenfeedClientWebSocket(OpenfeedClientConfigImpl config,
                                   OpenfeedClientEventHandler eventHandler,
                                   OpenfeedClientHandler clientHandler,
                                   OpenfeedClientMessageHandler messageHandler) {
        this.config = config;
        this.eventHandler = eventHandler;
        this.clientHandler = clientHandler;
        this.messageHandler = messageHandler;
        this.clientVersion = getClientVersion();
    }


    private String getClientVersion() {
        Package jarPackage = this.getClass().getPackage();
        String version = jarPackage.getImplementationVersion() != null ? jarPackage.getImplementationVersion() : "1.0.0";
        StringBuilder sb = new StringBuilder();
        sb.append("sdk-java"+":");
        sb.append(version + ":");
        sb.append(System.getProperty("java.version","") + ":");
        sb.append(System.getProperty("java.vendor","")+ ":");
        sb.append(System.getProperty("java.name","")+ ":");
        sb.append(System.getProperty("os.name","")+ ":");
        sb.append(System.getProperty("os.version","") + ":");
        sb.append(System.getProperty("os.arch",""));
        return sb.toString();
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
                log.info("{}: Attempting reconnection in {} ms", config.getClientId(),
                        config.getReconnectDelayMs());
                try {
                    Thread.sleep(config.getReconnectDelayMs());
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
            if (subReq != null && this.token != null) {
                // Use new correlationId
                subReq = subReq.toBuilder().setToken(this.token).build();
                // Save new request on Subscription
                sub.setRequest(subReq);
                OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
                send(req);
            }
        }
    }

    private void init() {
        uri = null;
        try {
            uri = new URI("ws://" + config.getHost() + ":" + config.getPort() + "/ws");
        } catch (URISyntaxException ex) {
            log.error("{}: Invalid URL err: {}", config.getClientId(), ex.getMessage());
        }
        log.info("{}: Initializing connection to: {} recBufSize: {}", config.getClientId(), uri,config.getReceiveBufferSize());
        // Connect with V13 (RFC 6455 aka HyBi-17).
        webSocketHandler = new OpenfeedWebSocketHandler(config, this, this.subscriptionManager, clientHandler, WebSocketClientHandshakerFactory
                .newHandshaker(uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders()),messageHandler);
        boolean epoll = OS.indexOf("linux") >= 0 ? true : false;

        // Ensure previous event loop was shutdown
        shutdown();
        // Configure the event loop
        if (epoll) {
            clientEventLoopGroup = new EpollEventLoopGroup();
        } else {
            clientEventLoopGroup = new NioEventLoopGroup();
        }
        log.debug("{}: Using EventLoop: {}", config.getClientId(), clientEventLoopGroup.getClass());
        try {
            clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientEventLoopGroup);
            if (epoll) {
                clientBootstrap.channel(EpollSocketChannel.class);
            } else {
                clientBootstrap.channel(NioSocketChannel.class);
            }
            clientBootstrap.option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MSEC)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    /*
                     * The default value is set by the /proc/sys/net/core/rmem_default file, and the
                     * maximum allowed value is set by the /proc/sys/net/core/rmem_max file.
                     */
                    .option(ChannelOption.SO_RCVBUF, config.getReceiveBufferSize())
                    //
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new HttpClientCodec());
                            p.addLast(new HttpObjectAggregator(512 * 1024));
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
            ChannelFuture connnectFuture = clientBootstrap.connect(config.getHost(), config.getPort()).sync();
            this.channel = connnectFuture.channel();
            // Wait for connect
            webSocketHandler.handshakeFuture().sync();
            this.connected.set(true);
            if (eventHandler != null) {
                eventHandler.onEvent(this, new OpenfeedEvent(EventType.Connected, "Connected to: " + uri));
            }
            // login
            login();

            log.info("{}: Successfully connected to: {} from: {}", config.getClientId(), uri, channel.localAddress());
        } catch (Exception e) {
            log.error("{}: Could not connect to uri {} err: {}", config.getClientId(), uri, e.getMessage());
            reconnectOrShutdown(config.isReconnect() ? false : true);
        }
    }

    private void reconnectOrShutdown(boolean shutdown) {
        closeConnection();
        if (!config.isReconnect() || shutdown) {
            if (!running.get()) {
                // Already shutdown
                return;
            }
            this.running.set(false);
            log.warn("{}: Closing and shutting down.", config.getClientId());
            shutdown();
        } else {
            log.info("{}: re-connecting in: {} ms", config.getClientId(), config.getReconnectDelayMs());
            reconnectInProgress.set(true);
        }
    }

    private void closeConnection() {
        if (this.channel != null && this.channel.isActive()) {
            this.channel.close();
        }
        if (eventHandler != null) {
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
        if (isLoggedIn()) {
            logout();
        }
        reconnectOrShutdown(config.isReconnect() ? false : true);
    }


    private void login() {
        LoginRequest request = LoginRequest.newBuilder().setCorrelationId(correlationId++)
                .setUsername(config.getUserName()).setPassword(config.getPassword()).setClientVersion(clientVersion).build();
        OpenfeedGatewayRequest ofreq = request().setLoginRequest(request).build();
        send(ofreq);
        this.loginFuture = this.channel.newPromise();
        try {
            boolean ret = this.loginFuture.await(LOGIN_WAIT_SEC, TimeUnit.SECONDS);
            if (!ret) {
                log.error("{}: Login timeout for user: ", config.getClientId(), request.getUsername());
            } else {
                numSuccessLogins++;
                if (eventHandler != null) {
                    eventHandler.onEvent(this, new OpenfeedEvent(EventType.Login, "Logged In"));
                }
            }
        } catch (InterruptedException e) {
            log.error("{}: Login Timeout err: {}", config.getClientId(), e.getMessage());
        }
    }

    private void log(OpenfeedGatewayRequest ofreq) {
        if(config.isLogRequestResponse()) {
            log.info("{} > {}", config.getClientId(), PbUtil.toJson(ofreq));
        }
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
        if (!isConnected()) {
            return;
        }
        log(req);
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

    public void completeLogin(boolean success, String error) {
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
        if (this.logoutFuture == null || logoutFuture.isDone()) {
            return;
        }
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
            log.debug("{}: instrumentRef: {}", config.getClientId(), s);
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
            log.debug("{}: instrumentRef: {}", config.getClientId(), id);
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
        log.debug("{}: instrumentRef Channel: {}", config.getClientId(), channelId);
        InstrumentReferenceRequest request = InstrumentReferenceRequest.newBuilder().setCorrelationId(correlationId++)
                .setChannelId(channelId).setToken(token).build();
        OpenfeedGatewayRequest req = request().setInstrumentReferenceRequest(request).build();
        send(req);
        return channel.newPromise();
    }

    @Override
    public String getSymbol(long marketId) {
        return marketIdToSymbol.get(marketId);
    }

    public void addMapping(InstrumentDefinition def) {
        this.marketIdToSymbol.put(def.getMarketId(), def.getSymbol());
        String ddfSymbol = InstrumentCache.getDdfSymbol(def);
        if (ddfSymbol != null) {
            this.marketIdToSymbol.put(def.getMarketId(), ddfSymbol);
        }
    }


    @Override
    public void exchangeRequest() {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        log.debug("{}: ExchangeReq : {}", config.getClientId());
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
        log.debug("{}: instrumentRef Exchange: {}", config.getClientId(), exchange);
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
        log.debug("{}: Subscribe Symbol: {}", config.getClientId(), Arrays.asList(syms.toArray()));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (String symbol : syms) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            // Subscription Type
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        if (request.getRequestsCount() == 0) {
            // already subscribed or no subscription items,  bail
            return null;
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, new SubscriptionType[]{subType}, syms.toArray(new String[0]));
        subscriptionManager.addSubscription(subscriptionId, subReq, symbols, correlationId);
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

        log.debug("{}: Subscribe Symbol: {}", config.getClientId(), Arrays.asList(syms.toArray()));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        // Subcription Types
        Set<SubscriptionType> subTypes = new HashSet<>();
        subTypes.addAll(Arrays.asList(subscriptionTypes));
        if (subTypes.size() == 0) {
            subTypes.add(SubscriptionType.QUOTE);
        }
        for (String symbol : syms) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            // Subscription Types
            subTypes.forEach(type -> subReq.addSubscriptionType(type));
            request.addRequests(subReq);
        }
        if (request.getRequestsCount() == 0) {
            // already subscribed or no subscription items,  bail
            return null;
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, subTypes.toArray(new SubscriptionType[0]), syms.toArray(new String[0]));
        subscriptionManager.addSubscription(subscriptionId, subReq, syms.toArray(new String[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }



    private String createSubscriptionId(String userName, Service service, SubscriptionType[] subscriptionTypes, String[] symbols) {
        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(":");
        sb.append(service.getNumber());
        sb.append(":");
        sb.append(subscriptionTypes);
        sb.append(":");
        sb.append(symbols);
        return sb.toString();
    }

    private String createSubscriptionId(String userName, Service service, SubscriptionType[] subscriptionTypes, Long[] ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(":");
        sb.append(service.getNumber());
        sb.append(":");
        sb.append(subscriptionTypes);
        sb.append(":");
        sb.append(ids);
        return sb.toString();
    }

    private String createSubscriptionId(String userName, Service service, SubscriptionRequest subscriptionRequest) {
        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(":");
        sb.append(service.getNumber());
        sb.append(":");
        sb.append(PbUtil.toJson(subscriptionRequest));
        return sb.toString();
    }

    private String createSubscriptionId(String userName, Service service, SubscriptionType[] subscriptionTypes, Integer[] ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(":");
        sb.append(service.getNumber());
        sb.append(":");
        sb.append(subscriptionTypes);
        sb.append(":");
        sb.append(ids);
        return sb.toString();
    }

    private String createSubscriptionId(String userName, Service service, String[] symbols) {
        StringBuilder sb = new StringBuilder();
        sb.append(userName);
        sb.append(":");
        sb.append(service.getNumber());
        sb.append(":");
        sb.append(symbols);
        return sb.toString();
    }

    @Override
    public String subscribe(Service service, SubscriptionType subscriptionType, long marketId) {
       return subscribe(service,subscriptionType,new long [] { marketId });
    }


    @Override
    public String subscribe(Service service, SubscriptionType subscriptionType, long[] marketIds) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<Long> ids = new HashSet<Long>();
        Arrays.stream(marketIds).forEach(id -> ids.add(id));

        SubscriptionType subType = subscriptionType != null ? subscriptionType : SubscriptionType.QUOTE;
        log.debug("{}: Subscribe Openfeed Id: {}", config.getClientId(), Arrays.asList(ids));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (Long id : ids) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, new SubscriptionType[]{subType}, ids.toArray(new Long[0]));
        subscriptionManager.addSubscription(subscriptionId, subReq, ids.toArray(new Long[0]), correlationId);
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
        Arrays.stream(marketIds).forEach(id -> ids.add(id));

        log.debug("{}: Subscribe Openfeed Id: {}", config.getClientId(), Arrays.asList(ids));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        // Subscription Types
        Set<SubscriptionType> subTypes = new HashSet<>();
        subTypes.addAll(Arrays.asList(subscriptionTypes));
        if (subTypes.size() == 0) {
            subTypes.add(SubscriptionType.QUOTE);
        }
        for (Long id : ids) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            // Subscription Types
            subTypes.forEach(type -> subReq.addSubscriptionType(type));
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, subTypes.toArray(new SubscriptionType[0]), ids.toArray(new Long[0]));
        subscriptionManager.addSubscription(subscriptionId, subReq, ids.toArray(new Long[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public void subscribe(SubscriptionRequest request) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        String subscriptionId = createSubscriptionId(config.getUserName(),request.getService(),request);
        subscriptionManager.addSubscription(subscriptionId, request);
        send(req);
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
            log.debug("{}: Subscribe Exchange: {}", config.getClientId(), exchange);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            // Subscription Type
            subReq.addSubscriptionType(subType);
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, new SubscriptionType[]{subType}, exchs.toArray(new String[0]));
        subscriptionManager.addSubscriptionExchange(subscriptionId, subReq, exchs.toArray(new String[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeExchange(Service service, SubscriptionType[] subscriptionTypes, String[] exchanges) {
        return subscribeExchange(service, subscriptionTypes, new InstrumentDefinition.InstrumentType[0], exchanges);
    }

    @Override
    public String subscribeExchange(Service service, SubscriptionType[] subscriptionTypes, InstrumentDefinition.InstrumentType[] instrumentTypes, String[] exchanges) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<String> exchs = new HashSet<>();
        exchs.addAll(Arrays.asList(exchanges));

        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        // Subscription Types
        Set<SubscriptionType> subTypes = new HashSet<>();
        subTypes.addAll(Arrays.asList(subscriptionTypes));
        for (String exchange : exchs) {
            log.debug("{}: Subscribe Exchange: {}", config.getClientId(), exchange);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            // Subscription Types
            subTypes.forEach(type -> subReq.addSubscriptionType(type));
            // Instrument Types
            Arrays.stream(instrumentTypes).forEach(type -> subReq.addInstrumentType(type));
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, subTypes.toArray(new SubscriptionType[0]), exchs.toArray(new String[0]));
        subscriptionManager.addSubscriptionExchange(subscriptionId, subReq, exchs.toArray(new String[0]), correlationId);
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
        Arrays.stream(channelIds).forEach(id -> ids.add(id));

        SubscriptionType subType = subscriptionType != null ? subscriptionType : SubscriptionType.QUOTE;
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        for (Integer id : ids) {
            log.debug("{}: Subscribe Channel: {}", config.getClientId(), id);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setChannelId(id);
            if (subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, new SubscriptionType[]{subType}, ids.toArray(new Integer[0]));
        subscriptionManager.addSubscriptionChannel(subscriptionId, subReq, ids.toArray(new Integer[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeChannel(Service service, SubscriptionType[] subscriptionTypes, int[] channelIds) {
        return subscribeChannel(service, subscriptionTypes, new InstrumentDefinition.InstrumentType[0], channelIds);
    }

    @Override
    public String subscribeChannel(Service service, SubscriptionType[] subscriptionTypes, InstrumentDefinition.InstrumentType[] instrumentTypes, int[] channelIds) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        Set<Integer> ids = new HashSet<>();
        Arrays.stream(channelIds).forEach(id -> ids.add(id));

        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);
        // Subscription Types
        Set<SubscriptionType> subTypes = new HashSet<>();
        subTypes.addAll(Arrays.asList(subscriptionTypes));
        for (Integer id : ids) {
            log.debug("{}: Subscribe Channel: {}", config.getClientId(), id);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setChannelId(id);
            subTypes.forEach(type -> subReq.addSubscriptionType(type));
            // Instrument Types
            Arrays.stream(instrumentTypes).forEach(type -> subReq.addInstrumentType(type));
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), service, subTypes.toArray(new SubscriptionType[0]), ids.toArray(new Integer[0]));
        subscriptionManager.addSubscriptionChannel(subscriptionId, subReq, ids.toArray(new Integer[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeSnapshot(String[] symbols, int intervalSec) {
        return subscribeSnapshot(Service.REAL_TIME_SNAPSHOT, symbols, intervalSec);
    }

    @Override
    public String subscribeSnapshot(Service service, String[] symbols, int intervalSec) {
        return subscribeSnapshot(service,null,symbols,intervalSec);
    }

    @Override
    public String subscribeSnapshot(Service service, SubscriptionType subscriptionType, String[] symbols, int intervalSec) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        // Eliminate dups
        List<String> syms = new ArrayList<>();

        Arrays.stream(symbols).forEach(s -> syms.add(s));

        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);

        for (String symbol : syms) {
            log.debug("{}: Subscribe Snapshot: {}", config.getClientId(), symbol);
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol)
                    .setSnapshotIntervalSeconds(intervalSec);
            if (subscriptionType != null) {
                subReq.addSubscriptionType(subscriptionType);
            }
            request.addRequests(subReq);
        }
        SubscriptionRequest subReq = request.build();
        String subscriptionId = createSubscriptionId(config.getUserName(), Service.REAL_TIME_SNAPSHOT, syms.toArray(new String[0]));
        subscriptionManager.addSubscription(subscriptionId, subReq, syms.toArray(new String[0]), correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(subReq).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public String subscribeSnapshot(Service service, SubscriptionType subscriptionType, long marketId, int intervalSec) {
        if (!isLoggedIn()) {
            throw new RuntimeException("Not logged in.");
        }
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service);

        log.debug("{}: Subscribe Snapshot: {}", config.getClientId(), marketId);
        Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(marketId).setSnapshotIntervalSeconds(intervalSec);
        if (subscriptionType != null) {
            subReq.addSubscriptionType(subscriptionType);
        }
        request.addRequests(subReq);
        String subscriptionId = createSubscriptionId(config.getUserName(), service, new SubscriptionType[]{subscriptionType}, new Long[]{marketId});
        subscriptionManager.addSubscription(subscriptionId, request.build(), new Long[]{marketId}, correlationId);
        OpenfeedGatewayRequest req = request().setSubscriptionRequest(request).build();
        send(req);
        return subscriptionId;
    }

    @Override
    public void unSubscribe(Service service, String[] symbols) {
        if (!isLoggedIn()) {
            return;
        }
        log.debug("{}: Un Subscribe Symbols: {}", config.getClientId(), Arrays.asList(symbols));
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
    public void unSubscribe(Service service, SubscriptionType subscriptionType, String[] symbols) {
        if (!isLoggedIn()) {
            return;
        }
        log.debug("{}: Un Subscribe Symbols: {} subType: {}", config.getClientId(), Arrays.asList(symbols), subscriptionType);
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String symbol : symbols) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setSymbol(symbol);
            if (subscriptionType != null) {
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
        log.debug("{}: Un Subscribe Ids: {}", config.getClientId(), Arrays.asList(marketIds));
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
    public void unSubscribe(Service service, SubscriptionType subscriptionType, long[] marketIds) {
        if (!isLoggedIn()) {
            return;
        }
        log.debug("{}: Un Subscribe Ids: {}", config.getClientId(), Arrays.asList(marketIds));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (Long id : marketIds) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setMarketId(id);
            if (subscriptionType != null) {
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
        log.debug("{}: Un Subscribe Exchanges: {}", config.getClientId(), Arrays.asList(exchanges));
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
    public void unSubscribeExchange(Service service, SubscriptionType subscriptionType, String[] exchanges) {
        if (!isLoggedIn()) {
            return;
        }
        log.debug("{}: Un Subscribe Exchanges: {}", config.getClientId(), Arrays.asList(exchanges));
        SubscriptionRequest.Builder request = SubscriptionRequest.newBuilder().setCorrelationId(correlationId++)
                .setToken(token).setService(service).setUnsubscribe(true);
        for (String exchange : exchanges) {
            Builder subReq = SubscriptionRequest.Request.newBuilder().setExchange(exchange);
            if (subscriptionType != null) {
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
        log.debug("{}: Un Subscribe Channel: {}", config.getClientId(), Arrays.asList(channelIds));
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
        return this.subscriptionManager.getSubscription(subscriptionId);
    }


}
