package org.openfeed.client.api.impl.websocket;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.openfeed.*;
import org.openfeed.client.api.OpenfeedClientHandler;
import org.openfeed.client.api.OpenfeedClientMessageHandler;
import org.openfeed.client.api.impl.ConnectionStats;
import org.openfeed.client.api.impl.OpenfeedClientConfigImpl;
import org.openfeed.client.api.impl.SubscriptionManagerImpl;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class OpenfeedWebSocketHandlerTest {

    private OpenfeedWebSocketHandler webSocketHandler;
    @Mock
    private OpenfeedClientConfigImpl config;
    @Mock
    private OpenfeedClientWebSocket client;
    @Mock
    private SubscriptionManagerImpl subscriptionManager;
    @Mock
    private WebSocketClientHandshaker webSocketClientHandshaker;
    @Mock
    private OpenfeedClientMessageHandler clientMessageHandler;

    private TestClientHandler clientHandler;

    @BeforeEach
    public void before() {
        config = new OpenfeedClientConfigImpl();
        clientHandler = new TestClientHandler();
        webSocketHandler = new OpenfeedWebSocketHandler(config,client,subscriptionManager, clientHandler,webSocketClientHandshaker, clientMessageHandler);
    }

    @Test
    public void parseMessage_V0() throws Exception {
        this.config.setProtocolVersion(0);

        Trades.Entry tradeEntry = Trades.Entry.newBuilder().setTrade(Trade.newBuilder().setPrice(100).setSide(BookSide.BID).setQuantity(10)).build();
        MarketUpdate.Builder update = MarketUpdate.newBuilder().setMarketId(1).setTrades(Trades.newBuilder().addTrades(tradeEntry));

        OpenfeedGatewayMessage.Builder b = OpenfeedGatewayMessage.newBuilder().setMarketUpdate(update.build());
        byte[] data = b.build().toByteArray();
        webSocketHandler.parseMessage(data.length,data);
        Thread.sleep(1000);
        assertNotNull(clientHandler.getMarketUpdate());
    }

    @Test
    public void parseMessage_V1() throws Exception {
        this.config.setProtocolVersion(1);

        VolumeAtPrice.Builder b = VolumeAtPrice.newBuilder();
        b.setMarketId(1);
        b.setSymbol("Test");
        for(int i = 0; i < 25_000; i++) {
            b.addPriceVolumes(VolumeAtPrice.PriceLevelVolume.newBuilder().setPrice(100+i).setVolume(1000 + i).build());
        }

        OpenfeedGatewayMessage.Builder ofgm = OpenfeedGatewayMessage.newBuilder().setVolumeAtPrice(b.build());
        byte[] data = ofgm.build().toByteArray();
        System.out.println("Len: "+data.length);
        ByteBuffer buf = ByteBuffer.allocate(2 + data.length);
        buf.putShort((short) data.length);
        buf.put(data);
        buf.flip();

        webSocketHandler.parseMessage(buf.limit(),buf.array());
        Thread.sleep(1000);
        assertNull(clientHandler.getMarketUpdate(),"Corrupt packet");
    }

    class TestClientHandler implements OpenfeedClientHandler {

        private MarketUpdate update;

        public MarketUpdate getMarketUpdate() {
             return this.update;
        }

        @Override
        public void onLoginResponse(LoginResponse loginResponse) {

        }

        @Override
        public void onLogoutResponse(LogoutResponse logoutResponse) {

        }

        @Override
        public void onInstrumentResponse(InstrumentResponse instrumentResponse) {

        }

        @Override
        public void onInstrumentReferenceResponse(InstrumentReferenceResponse instrumentReferenceResponse) {

        }

        @Override
        public void setInstrumentPromise(ChannelPromise promise) {

        }

        @Override
        public void onExchangeResponse(ExchangeResponse exchangeResponse) {

        }

        @Override
        public void onSubscriptionResponse(SubscriptionResponse subscriptionResponse) {

        }

        @Override
        public void onMarketStatus(MarketStatus marketStatus) {

        }

        @Override
        public void onHeartBeat(HeartBeat hb) {

        }

        @Override
        public void onInstrumentDefinition(InstrumentDefinition definition) {

        }

        @Override
        public void onMarketSnapshot(MarketSnapshot snapshot) {

        }

        @Override
        public void onMarketUpdate(MarketUpdate update) {
            System.out.println("update: bytes: "+update.getSerializedSize() + " msg: " +update);
            this.update = update;
        }

        @Override
        public void onVolumeAtPrice(VolumeAtPrice cumulativeVolume) {

        }

        @Override
        public void onOhlc(Ohlc ohlc) {

        }

        @Override
        public void onInstrumentAction(InstrumentAction instrumentAction) {

        }

        @Override
        public void onListSubscriptionsResponse(ListSubscriptionsResponse listSubscriptionsResponse) {

        }

        @Override
        public ConnectionStats getConnectionStats() {
            return null;
        }
    }
}
