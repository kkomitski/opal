package com.github.kkomits.opal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import com.github.kkomitski.opal.utils.Market;
import com.github.kkomitski.opal.utils.MarketsLoader;

/**
 * Outputs a binary file of randomized orders, to be used for stress testing the
 * matching engine
 */
public class StaticRequestsBuilder {

    // Configuration constant for output file size in MB
    private static final int FILE_SIZE_MB = 500;

    // Precomputed cumulative weights for weighted random selection
    private double[] instrumentCumulativeWeights = null;
    private double totalWeight = 0.0;
    private Market[] markets = null;
    private final Random random = new Random();
    private int orderSideToggle = 0; // For alternating bid/ask

    public static void main(String[] args) throws IOException {
        StaticRequestsBuilder builder = new StaticRequestsBuilder();
        String marketsLink = args.length > 0 ? args[0] : "http://localhost:8080/markets";
        builder.buildStaticFile(marketsLink);
    }

    public void buildStaticFile(String marketsLink) throws IOException {
        // Load markets from URL or file path
        this.markets = MarketsLoader.load(marketsLink);

        // Precompute cumulative weights for weighted random selection
        if (markets != null && markets.length > 0) {
            instrumentCumulativeWeights = new double[markets.length];
            double sum = 0.0;
            for (int i = 0; i < markets.length; i++) {
                sum += (double) markets[i].book_depth * (double) markets[i].level_depth;
                instrumentCumulativeWeights[i] = sum;
            }
            totalWeight = sum;
        }

        // Calculate number of orders based on file size
        long numOrders = (FILE_SIZE_MB * 1024L * 1024L) / 11;
        System.out.println("Generating " + numOrders + " orders for ~" + FILE_SIZE_MB + " MB file...");

        // Write orders to binary file
        try (FileOutputStream fos = new FileOutputStream("orders.bin")) {
            for (long i = 0; i < numOrders; i++) {
                byte[] orderBytes = generateRandomOrder();
                fos.write(orderBytes);
            }
        }

        System.out.println("Static orders file 'orders.bin' created successfully.");
    }

    private byte[] generateRandomOrder() {
        final double OUTLIER_PROBABILITY = 0.0; // Probability of allowing dramatic outliers
        final double CLIENT_VOLATILITY_SCALE = 1.0; // Scale factor for client's Gaussian distribution
        final int PRICE_BIAS = 0;

        // Weighted random selection of instrument index based on book_depth * level_depth
        int instrumentIndex;
        if (markets != null && markets.length > 0 && instrumentCumulativeWeights != null) {
            double r = random.nextDouble() * totalWeight;
            int lo = 0, hi = instrumentCumulativeWeights.length - 1;
            while (lo < hi) {
                int mid = (lo + hi) / 2;
                if (r < instrumentCumulativeWeights[mid]) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            instrumentIndex = lo;
        } else {
            instrumentIndex = random.nextInt(25);
        }

        String symbol = markets != null && markets.length > instrumentIndex ? markets[instrumentIndex].symbol
            : getSymbolForIndex(instrumentIndex);
        int basePrice = markets != null && markets.length > instrumentIndex ? markets[instrumentIndex].price
            : 10000;
        int levelDepth = markets != null && markets.length > instrumentIndex ? markets[instrumentIndex].level_depth
            : 100;
        int bookDepth = markets != null && markets.length > instrumentIndex ? markets[instrumentIndex].book_depth
            : 50;
        bookDepth = bookDepth / 2; // Divide by 2 as per user request

        // Alternate every order: even = bid, odd = ask
        boolean isBid = orderSideToggle % 2 == 0;
        orderSideToggle = (orderSideToggle + 1) % 2;

        // Generate prices with a Gaussian distribution
        double sigma = (bookDepth / 6.0) * CLIENT_VOLATILITY_SCALE;
        double gaussian = random.nextGaussian(); // mean=0, stddev=1
        int priceOffset = (int) (gaussian * sigma);
        if (random.nextDouble() >= OUTLIER_PROBABILITY) {
            priceOffset = Math.max(-bookDepth, Math.min(bookDepth, priceOffset)); // Clamp to bookDepth
        }
        int price = basePrice + priceOffset + PRICE_BIAS;

        // Quantity scaled by level depth (keep within 1..32767)
        int maxQty = Math.min(32767, Math.max(1, levelDepth * 10));
        short quantity = (short) (random.nextInt(maxQty) + 1);
        int orderId = random.nextInt(Integer.MAX_VALUE);

        // Encode order using the protocol
        return encodeOrder(instrumentIndex, price, quantity, isBid, orderId);
    }

    // Map instrument index to symbol (must match orderBooks in server)
    private String getSymbolForIndex(int index) {
        String[] symbols = {
                "MSFT", "AAPL", "GOOG", "AMZN", "META", "TSLA", "NVDA", "NFLX", "BABA", "ORCL",
                "INTC", "CSCO", "IBM", "ADBE", "CRM", "PYPL", "UBER", "LYFT", "SHOP", "SQ",
                "TWTR", "SNAP", "ZM", "SPOT", "ROKU"
        };
        if (index >= 0 && index < symbols.length)
            return symbols[index];
        return "MSFT";
    }

    private byte[] encodeOrder(int instrumentIndex, int price, short quantity, boolean isBid, int orderId) {
        byte[] bytes = new byte[11];

        // First 2 bytes: MSB for bid/ask and 15 bits for instrument index
        int header = (((isBid ? 1 : 0) << 15) | (instrumentIndex & 0x7FFF));
        bytes[0] = (byte) (header >>> 8);
        bytes[1] = (byte) header;

        // Next 3 bytes: price (24 bits)
        bytes[2] = (byte) (price >>> 16);
        bytes[3] = (byte) (price >>> 8);
        bytes[4] = (byte) price;

        // Next 2 bytes: quantity (16 bits)
        bytes[5] = (byte) (quantity >>> 8);
        bytes[6] = (byte) quantity;

        // Next 4 bytes: order id
        bytes[7] = (byte) (orderId >>> 24);
        bytes[8] = (byte) (orderId >>> 16);
        bytes[9] = (byte) (orderId >>> 8);
        bytes[10] = (byte) orderId;

        return bytes;
    }
}
