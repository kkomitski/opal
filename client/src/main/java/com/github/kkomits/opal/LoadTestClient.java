package com.github.kkomits.opal;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.github.kkomitski.opal.utils.Market;
import com.github.kkomitski.opal.utils.MarketsLoader;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class LoadTestClient {

    // --- CONFIGURATION HANDLES ---

    // Per-instrument price deviation (set from markets.xml: limitsPerBook / 2)
    private int[] maxPriceDeviationPerInstrument = null;

    // The gap between the best bid and best ask in ticks (simulated)
    private static final int TARGET_SPREAD = 1;

    // Bias to move the price: -1.0 (strong sell/down) to 1.0 (strong buy/up)
    // 0.0 is neutral random walk. 0.1 is slight upward drift.
    private static final double PRICE_BIAS = 0.1;

    // Controls the standard deviation of the order price distribution.
    // Higher value -> steeper curve (more orders concentrated at the spread)
    // Lower value -> flatter curve (orders spread out more)
    private static final double CURVE_STEEPNESS_FACTOR = 25;

    // Oscillating spread cross probability
    private static final double SPREAD_CROSS_PROBABILITY_MIN = 0.2;
    private static final double SPREAD_CROSS_PROBABILITY_MAX = 1.0;
    private static final double SPREAD_CROSS_OSCILLATION_PERIOD_SEC = 3; // seconds

    // Probability (0.0 - 1.0) of generating an order far from the current price
    private static final double OUTLIER_PROBABILITY = 0.0;

    // Ratio of bid orders (0.5 = balanced, 0.6 = 60% bids/40% asks, reduces ask liquidity)
    private static final double BID_RATIO = 0.5;

    // Probability (0.0 - 1.0) of sending market orders instead of limit orders
    private static final double MARKET_ORDER_PROBABILITY = 0.525;

    // Volatility factor: How much the "market price" can move per update step
    // relative to the book depth.
    private static final double VOLATILITY_FACTOR = 0.05;

    // -----------------------------

    private final String host;
    private final int port;
    private final int numConnections;
    private final int ordersPerSecond;
    private final int testDurationSeconds;

    private final List<Channel> channels = new ArrayList<>();
    private final AtomicLong ordersSent = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final Random random = new Random();
    // private final AtomicInteger orderSideToggle = new AtomicInteger(0);

    private Market[] markets = null;
    // Per-instrument max order quantity (set to ordersPerLimit / 2)
    private int[] maxOrderQtyPerInstrument = null;

    // Shared state to track the "current price" of each instrument across all
    // threads
    // This allows the simulated market to "move" collectively
    private AtomicInteger[] instrumentCurrentPrices;

    private double[] instrumentCumulativeWeights = null;
    private double totalWeight = 0.0;

    private EventLoopGroup workerGroup;
    private ScheduledExecutorService scheduler;

    public LoadTestClient(String host, int port, int numConnections, int ordersPerSecond, int testDurationSeconds) {
        this.host = host;
        this.port = port;
        this.numConnections = numConnections;
        this.ordersPerSecond = ordersPerSecond;
        this.testDurationSeconds = testDurationSeconds;
    }

    // Track test start time for oscillation
    private long testStartTimeMillis = 0;

    public void start(String marketsLink) throws Exception {
        // Load markets from URL or file path
        this.markets = MarketsLoader.load(marketsLink);

        // Initialize market state and per-instrument parameters
        if (this.markets != null && this.markets.length > 0) {
            initMarketState();
            // Set per-instrument price deviation and max order quantity
            maxPriceDeviationPerInstrument = new int[markets.length];
            maxOrderQtyPerInstrument = new int[markets.length];
            for (int i = 0; i < markets.length; i++) {
                maxPriceDeviationPerInstrument[i] = Math.max(1, markets[i].limitsPerBook / 2);
                maxOrderQtyPerInstrument[i] = Math.max(1, markets[i].ordersPerLimit / 2);
            }
        } else {
            // Fallback for no markets loaded (shouldn't happen with correct usage)
            this.instrumentCurrentPrices = new AtomicInteger[25];
            maxPriceDeviationPerInstrument = new int[25];
            maxOrderQtyPerInstrument = new int[25];
            for (int i = 0; i < 25; i++) {
                this.instrumentCurrentPrices[i] = new AtomicInteger(10000);
                maxPriceDeviationPerInstrument[i] = 100;
                maxOrderQtyPerInstrument[i] = 1000;
            }
        }

        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        scheduler = Executors.newScheduledThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));

        System.out.println("Starting load test client V2...");
        System.out.println("Host: " + host + ":" + port);
        System.out.println("Connections: " + numConnections);
        System.out.println("Target orders/sec: " + ordersPerSecond);
        System.out.println("Duration: " + testDurationSeconds + " seconds");
        System.out.println(
                "Scenario: Spread=" + TARGET_SPREAD + ", Bias=" + PRICE_BIAS + ", Steepness=" + CURVE_STEEPNESS_FACTOR + ", BidRatio=" + BID_RATIO + ", MarketProb=" + MARKET_ORDER_PROBABILITY);
        System.out.println("----------------------------------------");

        // Create all connections
        CountDownLatch connectLatch = new CountDownLatch(numConnections);
        for (int i = 0; i < numConnections; i++) {
            createConnection(connectLatch);
            if (i % 50 == 0 && i > 0)
                Thread.sleep(10); // Ramp up
        }

        System.out.println("Waiting for connections...");
        if (!connectLatch.await(30, TimeUnit.SECONDS)) {
            System.err.println("Timeout waiting for connections!");
        }
        System.out.println("Active connections: " + activeConnections.get());

        // Start sending orders
        testStartTimeMillis = System.currentTimeMillis();
        startSendingOrders();

        // Start metrics & market walk
        startMetricsReporting(testStartTimeMillis);
        startMarketSimulation();

        // Wait for test duration
        Thread.sleep(testDurationSeconds * 1000L);

        shutdown();
        printFinalReport(testStartTimeMillis);
    }

    private void initMarketState() {
        instrumentCumulativeWeights = new double[markets.length];
        instrumentCurrentPrices = new AtomicInteger[markets.length];

        double sum = 0.0;
        for (int i = 0; i < markets.length; i++) {
            // Weight probability by liquidity depth (more liquid = more activity)
            sum += (double) markets[i].limitsPerBook * (double) markets[i].ordersPerLimit;
            instrumentCumulativeWeights[i] = sum;

            // Initialize current price from config
            instrumentCurrentPrices[i] = new AtomicInteger(markets[i].price);
        }
        totalWeight = sum;
    }

    private void createConnection(CountDownLatch latch) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ClientHandler());
                    }
                });

        bootstrap.connect(host, port).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                channels.add(f.channel());
                activeConnections.incrementAndGet();
            }
            latch.countDown();
        });
    }

    private void startSendingOrders() {
        int ordersPerConnection = ordersPerSecond / Math.max(1, activeConnections.get());
        // Use microsecond scheduling for better granularity if possible, but scheduler
        // is millis based.
        // If rate is high, we batch or loop. Here we stick to simple interval.
        long delayMicros = 1_000_000L / Math.max(1, ordersPerConnection);
        long delayMillis = delayMicros / 1000;
        int batchSize = 1;

        if (delayMillis == 0) {
            delayMillis = 1;
            batchSize = (int) Math.ceil((double) ordersPerConnection / 1000.0);
        }

        System.out.println("Sending ~" + ordersPerConnection + " orders/sec per connection (Batch: " + batchSize + ")");

        final int finalBatchSize = batchSize;
        for (Channel channel : channels) {
            scheduler.scheduleAtFixedRate(() -> {
                if (channel.isActive()) {
                    for (int k = 0; k < finalBatchSize; k++) {
                        sendRandomOrder(channel);
                    }
                }
            }, 0, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    // Separate thread to simulate market price movement (Random Walk with Drift)
    private void startMarketSimulation() {
        scheduler.scheduleAtFixedRate(() -> {
            if (markets == null)
                return;
            for (int i = 0; i < markets.length; i++) {
                int currentPrice = instrumentCurrentPrices[i].get();
                int bookDepth = markets[i].limitsPerBook;

                // Random Walk Step
                double noise = random.nextGaussian() * (bookDepth * 0.01 * VOLATILITY_FACTOR); // 1% of depth * vol
                                                                                               // factor
                double bias = bookDepth * 0.005 * PRICE_BIAS; // small directional bias per tick

                int move = (int) Math.round(noise + bias);

                // Add some mean reversion to keep it somewhat sane if needed, or bounded
                // Here we just let it walk but maybe clamp slightly if it goes wild?
                // For simulated load test, uncontrolled walk is fine, matches "volatile"
                // markets.

                int newPrice = currentPrice + move;
                
                // Clamp mechanism to keep price within reasonable bounds of the initial price
                // This prevents the random walk from hitting the engine's hard limits (e.g. ±500 ticks)
                int startPrice = markets[i].price;
                int upperLimit = startPrice + 300; // Keep within 300 ticks
                int lowerLimit = Math.max(1, startPrice - 300);
                
                if (newPrice > upperLimit) newPrice = upperLimit;
                if (newPrice < lowerLimit) newPrice = lowerLimit;

                instrumentCurrentPrices[i].set(newPrice);
            }
        }, 0, 100, TimeUnit.MILLISECONDS); // Update price 10 times a second
    }

    private double getOscillatingSpreadCrossProbability() {
        double elapsedSec = (System.currentTimeMillis() - testStartTimeMillis) / 1000.0;
        double phase = (elapsedSec % SPREAD_CROSS_OSCILLATION_PERIOD_SEC) / SPREAD_CROSS_OSCILLATION_PERIOD_SEC;
        double sine = Math.sin(2 * Math.PI * phase);
        double prob = SPREAD_CROSS_PROBABILITY_MIN + (SPREAD_CROSS_PROBABILITY_MAX - SPREAD_CROSS_PROBABILITY_MIN) * (0.5 + 0.5 * sine);
        return prob;
    }

    private void sendRandomOrder(Channel channel) {
        try {
            // 1. Pick Instrument
            int instrumentIndex = 0;
            if (markets != null && markets.length > 0) {
                double r = random.nextDouble() * totalWeight;
                int lo = 0, hi = instrumentCumulativeWeights.length - 1;
                while (lo < hi) {
                    int mid = (lo + hi) / 2;
                    if (r < instrumentCumulativeWeights[mid])
                        hi = mid;
                    else
                        lo = mid + 1;
                }
                instrumentIndex = lo;
            } else {
                instrumentIndex = random.nextInt(25);
                // Fallback dummy price if simple 25-instrument mode
                if (instrumentCurrentPrices == null || instrumentCurrentPrices.length <= instrumentIndex) {
                    // Safe handling if index out of bounds or not init
                     // (Though init should handle it)
                }
            }

            int currentPrice = (instrumentCurrentPrices != null && instrumentCurrentPrices.length > instrumentIndex)
                    ? instrumentCurrentPrices[instrumentIndex].get()
                    : 10000;

            Market m = (markets != null && markets.length > instrumentIndex) ? markets[instrumentIndex] : null;
            int bookDepth = (m != null) ? m.limitsPerBook : 1000;
            int levelDepth = (m != null) ? m.ordersPerLimit : 1000;
            int maxPriceDeviation = (maxPriceDeviationPerInstrument != null && maxPriceDeviationPerInstrument.length > instrumentIndex)
                ? maxPriceDeviationPerInstrument[instrumentIndex] : 100;
            int maxOrderQty = (maxOrderQtyPerInstrument != null && maxOrderQtyPerInstrument.length > instrumentIndex)
                ? maxOrderQtyPerInstrument[instrumentIndex] : 1000;

            // 2. Determine Side (Bid/Ask)
            boolean isBid = random.nextDouble() < BID_RATIO;

            // 3. Determine Price
            int price;
            boolean isOutlier = random.nextDouble() < OUTLIER_PROBABILITY;
            double spreadCrossProb = getOscillatingSpreadCrossProbability();
            boolean crossSpread = random.nextDouble() < spreadCrossProb;
            boolean isMarket = random.nextDouble() < MARKET_ORDER_PROBABILITY;


            if (isMarket) {
                 // Market order: price = 0
                 price = 0;
            } else if (isOutlier) {
                 // Generate deep OTM order, clamped to maxPriceDeviation
                 int range = Math.min(maxPriceDeviation, bookDepth * 2);
                 int offset = random.nextInt(range) + (int) (range * 0.3);
                 price = isBid ? currentPrice - offset : currentPrice + offset;
            } else if (crossSpread) {
                 // Aggressive order crossing the spread, with small random offset to prevent flooding single levels
                 if (isBid) {
                     price = currentPrice + 1 + random.nextInt(3); 
                 } else {
                     price = currentPrice - 1 - random.nextInt(3); 
                 }
            } else {
                 // Passive order - Bell curve centered at the spread
                 int effectiveDepth = Math.min(maxPriceDeviation, bookDepth);
                 double sigma = effectiveDepth / CURVE_STEEPNESS_FACTOR;

                 double dist = Math.abs(random.nextGaussian()) * sigma;
                 dist = Math.min(dist, maxPriceDeviation);

                 if (isBid) {
                     price = (int) (currentPrice - dist);
                 } else {
                     price = (int) (currentPrice + dist);
                 }
            }

            // Final safety clamp relative to Current Price
            // Calculate the valid price collar (centered around currentPrice, width = bookDepth)
            if (!isMarket) {
                int halfBook = bookDepth / 2;
                int collarMin = Math.max(1, currentPrice - halfBook);
                int collarMax = currentPrice + halfBook;
                price = Math.max(collarMin, Math.min(collarMax, price));
                
                // Hard clamp against zero
                if (price <= 0)
                    price = 1;
            }

            // 4. Determine Quantity (exponential decay from spread)
            double distFromSpread = Math
                    .abs(price - (isBid ? currentPrice - TARGET_SPREAD / 2 : currentPrice + TARGET_SPREAD / 2));
            double decay = Math.exp(-3.0 * (distFromSpread / (double) maxPriceDeviation));

            int baseQty = (int) (maxOrderQty * decay);

            int quantityInt = Math.max(1, (int) (baseQty * (0.5 + random.nextDouble())));
            short quantity = (short) Math.min(32767, quantityInt);

            int orderId = random.nextInt(Integer.MAX_VALUE);

            // 5. Send
            byte[] orderBytes = encodeOrder(instrumentIndex, price, quantity, isBid, orderId);
            ByteBuf buffer = Unpooled.wrappedBuffer(orderBytes);
            channel.writeAndFlush(buffer).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    ordersSent.incrementAndGet();
                    bytesSent.addAndGet(orderBytes.length);
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] encodeOrder(int instrumentIndex, int price, short quantity, boolean isBid, int orderId) {
        byte[] bytes = new byte[11];
        // Header: Side (1 bit) + Index (15 bits)
        int header = (((isBid ? 1 : 0) << 15) | (instrumentIndex & 0x7FFF));
        bytes[0] = (byte) (header >>> 8);
        bytes[1] = (byte) header;

        // Price (24 bits)
        bytes[2] = (byte) (price >>> 16);
        bytes[3] = (byte) (price >>> 8);
        bytes[4] = (byte) price;

        // Quantity (16 bits)
        bytes[5] = (byte) (quantity >>> 8);
        bytes[6] = (byte) quantity;

        // Order ID (32 bits)
        bytes[7] = (byte) (orderId >>> 24);
        bytes[8] = (byte) (orderId >>> 16);
        bytes[9] = (byte) (orderId >>> 8);
        bytes[10] = (byte) orderId;

        return bytes;
    }

    // Helper for fallback symbol lookup
    private String getSymbolForIndex(int index) {
        return "UNK" + index;
    }

    private void startMetricsReporting(long startTime) {
        scheduler.scheduleAtFixedRate(() -> {
            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            if (elapsedSeconds == 0)
            return;
            long currentOrders = ordersSent.get();
            double rate = (double) currentOrders / elapsedSeconds;

            // Show price of first market as sample
            int samplePrice = (instrumentCurrentPrices != null && instrumentCurrentPrices.length > 0)
                ? instrumentCurrentPrices[0].get()
                : 0;

            System.out.printf("[%ds] Sent: %,d | Rate: %.0f/s | Conns: %d | Simulated Price: %d%n",
                elapsedSeconds, currentOrders, rate, activeConnections.get(), samplePrice);
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void shutdown() {
        System.out.println("Shutting down client...");
        for (Channel c : channels) {
            if (c.isOpen())
                c.close();
        }
        if (scheduler != null)
            scheduler.shutdownNow();
        if (workerGroup != null)
            workerGroup.shutdownGracefully();
    }

    private void printFinalReport(long startTime) {
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Test Finished in " + elapsed + "ms");
        System.out.println("Total Orders: " + ordersSent.get());
    }

    private static class ClientHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
