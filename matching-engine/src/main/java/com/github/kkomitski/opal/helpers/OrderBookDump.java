package com.github.kkomitski.opal.helpers;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.orderbook.Limit;
import com.github.kkomitski.opal.orderbook.LimitChunkPool;
import com.github.kkomitski.opal.orderbook.LimitPool;

public class OrderBookDump {

    public static void generateHtml(OrderBook book, String filename) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("<!DOCTYPE html>");
            writer.println("<html lang='en'>");
            writer.println("<head>");
            writer.println("<meta charset='UTF-8'>");
            writer.println("<title>OrderBook Dump - " + book.getName() + "</title>");
            writer.println("<style>");
            writer.println("body { background-color: #1e1e1e; color: #d4d4d4; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; }");
            writer.println(".container { max-width: 1200px; margin: 0 auto; display: grid; grid-template-columns: 300px 1fr; gap: 20px; }");
            writer.println(".stats-panel { background-color: #252526; padding: 20px; border-radius: 8px; border: 1px solid #3e3e42; height: fit-content; position: sticky; top: 20px; }");
            writer.println(".book-panel { background-color: #252526; border-radius: 8px; border: 1px solid #3e3e42; overflow: hidden; }");
            writer.println("h1, h2, h3 { color: #ffffff; margin-top: 0; }");
            writer.println(".stat-row { display: flex; justify-content: space-between; margin-bottom: 8px; border-bottom: 1px solid #3e3e42; padding-bottom: 4px; }");
            writer.println(".stat-label { color: #858585; }");
            writer.println(".stat-value { font-weight: bold; }");
            writer.println(".order-table { width: 100%; border-collapse: collapse; }");
            writer.println(".order-table th, .order-table td { padding: 8px 12px; text-align: right; }");
            writer.println(".order-table th { background-color: #333333; color: #cccccc; font-weight: 600; text-align: center; }");
            writer.println(".ask-row { background-color: rgba(255, 99, 71, 0.1); color: #ff6b6b; }");
            writer.println(".ask-row:hover { background-color: rgba(255, 99, 71, 0.2); }");
            writer.println(".bid-row { background-color: rgba(60, 179, 113, 0.1); color: #4ec9b0; }");
            writer.println(".bid-row:hover { background-color: rgba(60, 179, 113, 0.2); }");
            writer.println(".spread-row { background-color: #2d2d30; color: #ffd700; font-weight: bold; text-align: center; padding: 15px; border-top: 2px solid #555; border-bottom: 2px solid #555; }");
            writer.println(".bar-container { background-color: #333; height: 4px; width: 100%; margin-top: 5px; border-radius: 2px; overflow: hidden; }");
            writer.println(".bar-fill { height: 100%; transition: width 0.3s ease; }");
            writer.println(".ask-fill { background-color: #ff6b6b; }");
            writer.println(".bid-fill { background-color: #4ec9b0; }");
            writer.println("</style>");
            writer.println("</head>");
            writer.println("<body>");

            // Stats Calculation
            int bestBid = book.getBestBid();
            int bestAsk = book.getBestAsk();
            boolean crossed = bestBid >= bestAsk && bestAsk != 0 && bestBid != 0;
            int spread = (bestAsk != 0 && bestBid != 0) ? bestAsk - bestBid : 0;
            int midPrice = (bestAsk != 0 && bestBid != 0) ? (bestAsk + bestBid) / 2 : 0;
            
            // Collect and Sort Asks (lowest price first - close to spread)
            Map<Integer, Limit> askLimits = book.getAskLimits();
            List<Integer> askPrices = new ArrayList<>(askLimits.keySet());
            Collections.sort(askPrices); // Ascending: 124, 125, 126... but we want highest on top visually usually, but for table list:
            // Standard order book view: High Asks at top -> Low Asks -> Low Bids -> High Bids? 
            // Usually: 
            // Asks (High to Low)
            // -- SPREAD --
            // Bids (High to Low)
            
            Collections.sort(askPrices, Collections.reverseOrder()); // Highest ask at top of list
            
            // Collect and Sort Bids (highest price first - close to spread)
            Map<Integer, Limit> bidLimits = book.getBidLimits();
            List<Integer> bidPrices = new ArrayList<>(bidLimits.keySet());
            Collections.sort(bidPrices, Collections.reverseOrder()); // Highest bid at top (closest to spread)

            int totalBids = bidLimits.size();
            int totalAsks = askLimits.size();
            long totalBidVolume = bidLimits.values().stream().mapToLong(Limit::getTotalVolume).sum();
            long totalAskVolume = askLimits.values().stream().mapToLong(Limit::getTotalVolume).sum();

            // Pool Stats
            LimitPool limitPool = book.getLimitPool();
            int limitPoolActive = limitPool.getActiveCount();
            int limitPoolCapacity = limitPool.getCapacity();
            double limitPoolUsage = (double) limitPoolActive / limitPoolCapacity * 100;

            LimitChunkPool chunkPool = limitPool.getLimitChunkPool();
            int chunkPoolActive = chunkPool.getActiveCount();
            int chunkPoolCapacity = chunkPool.getCapacity();
            double chunkPoolUsage = (double) chunkPoolActive / chunkPoolCapacity * 100;
            
            // ---- Container ----
            writer.println("<div class='container'>");
            
            // ---- Stats Panel ----
            writer.println("<div class='stats-panel'>");
            writer.println("<h2>Book Stats</h2>");
            
            writeStat(writer, "Instrument", book.getName());
            writeStat(writer, "ID", String.valueOf(book.getInstrumentIndex()));
            writeStat(writer, "Timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME));
            writer.println("<hr style='border-color: #3e3e42; opacity: 0.5;'>");
            
            writeStat(writer, "Best Ask", bestAsk == 0 ? "-" : String.valueOf(bestAsk));
            writeStat(writer, "Best Bid", bestBid == 0 ? "-" : String.valueOf(bestBid));
            writeStat(writer, "Spread", String.valueOf(spread));
            writeStat(writer, "Mid Price", String.valueOf(midPrice));
            writeStat(writer, "Crossed?", crossed ? "<span style='color:red'>YES</span>" : "No");
            
            writer.println("<hr style='border-color: #3e3e42; opacity: 0.5;'>");
            writeStat(writer, "Bid Levels", String.valueOf(totalBids));
            writeStat(writer, "Ask Levels", String.valueOf(totalAsks));
            writeStat(writer, "Total Bid Vol", String.valueOf(totalBidVolume));
            writeStat(writer, "Total Ask Vol", String.valueOf(totalAskVolume));
            
            writer.println("<hr style='border-color: #3e3e42; opacity: 0.5;'>");
            writeStat(writer, "Disruptor Usage", String.format("%.2f%%", book.getDisruptorUsage() * 100));
            writeStat(writer, "Ring Buffer", String.valueOf(book.getRingBufferCapacity()));

            writer.println("<hr style='border-color: #3e3e42; opacity: 0.5;'>");
            writeStat(writer, "Limit Pool Usage", String.format("%.2f%% (%d/%d)", limitPoolUsage, limitPoolActive, limitPoolCapacity));
            writeStat(writer, "Chunk Pool Usage", String.format("%.2f%% (%d/%d)", chunkPoolUsage, chunkPoolActive, chunkPoolCapacity));
            
            writer.println("</div>"); // End Stats Panel

            // ---- Book Panel ----
            writer.println("<div class='book-panel'>");
            writer.println("<table class='order-table'>");
            writer.println("<thead><tr><th>Count</th><th>Volume</th><th>Price</th></tr></thead>");
            writer.println("<tbody>");
            

            // Render Asks (show 9 closest to spread, then a 'more' row, then the farthest outlier)
            long maxVol = Math.max(
                bidLimits.values().stream().mapToLong(Limit::getTotalVolume).max().orElse(1),
                askLimits.values().stream().mapToLong(Limit::getTotalVolume).max().orElse(1)
            );

            int askCount = askPrices.size();
            int askShow = Math.min(9, askCount);
            // 9 closest to spread (lowest ask prices)
            for (int i = 0; i < askShow; i++) {
                int price = askPrices.get(i);
                Limit limit = askLimits.get(price);
                renderRow(writer, limit, price, maxVol, "ask");
            }
            // If more than 11, show 'more' row and outlier
            if (askCount > 10) {
                int moreCount = askCount - 10;
                int moreStart = askShow;
                int outlierIdx = askCount - 1;
                // 'More' row
                writer.println("<tr class='ask-row'><td colspan='3' style='text-align:center; color:#ffb366;'>+" + moreCount + " more asks...</td></tr>");
                // Outlier row (farthest ask)
                int price = askPrices.get(outlierIdx);
                Limit limit = askLimits.get(price);
                renderRow(writer, limit, price, maxVol, "ask");
            } else if (askCount > 9) {
                // If exactly 10 or 11, just show them all
                for (int i = askShow; i < askCount; i++) {
                    int price = askPrices.get(i);
                    Limit limit = askLimits.get(price);
                    renderRow(writer, limit, price, maxVol, "ask");
                }
            }


            // Render Spread with Mid Price
            writer.println("<tr><td colspan='3' class='spread-row' style='position:relative;'>");
            writer.println("<div style='display:flex; justify-content:center; align-items:center; width:100%; height:100%;'>");
            if (crossed) {
                writer.println("<span style='color: #ff6b6b; margin-right:16px;'>MARKET CROSSED (" + spread + ")</span>");
            } else {
                writer.println("<span style='margin-right:16px;'>SPREAD: " + spread + "</span>");
            }
            writer.println("<span style='background:#333; color:#ffd700; border-radius:6px; padding:4px 12px; font-size:1.1em; font-weight:bold; margin-left:16px;'>Mid: " + midPrice + "</span>");
            writer.println("</div>");
            writer.println("</td></tr>");

            // Render Bids (show 9 closest to spread, then a 'more' row, then the farthest outlier)
            int bidCount = bidPrices.size();
            int bidShow = Math.min(9, bidCount);
            for (int i = 0; i < bidShow; i++) {
                int price = bidPrices.get(i);
                Limit limit = bidLimits.get(price);
                renderRow(writer, limit, price, maxVol, "bid");
            }
            if (bidCount > 10) {
                int moreCount = bidCount - 10;
                int moreStart = bidShow;
                int outlierIdx = bidCount - 1;
                writer.println("<tr class='bid-row'><td colspan='3' style='text-align:center; color:#b3e6cc;'>+" + moreCount + " more bids...</td></tr>");
                int price = bidPrices.get(outlierIdx);
                Limit limit = bidLimits.get(price);
                renderRow(writer, limit, price, maxVol, "bid");
            } else if (bidCount > 9) {
                for (int i = bidShow; i < bidCount; i++) {
                    int price = bidPrices.get(i);
                    Limit limit = bidLimits.get(price);
                    renderRow(writer, limit, price, maxVol, "bid");
                }
            }

            writer.println("</tbody>");
            writer.println("</table>");
            writer.println("</div>"); // End Book Panel
            
            writer.println("</div>"); // End Container
            writer.println("</body></html>");
            
            // System.out.println("Dumped OrderBook to " + filename);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeStat(PrintWriter writer, String label, String value) {
        writer.println("<div class='stat-row'>");
        writer.println("<span class='stat-label'>" + label + "</span>");
        writer.println("<span class='stat-value'>" + value + "</span>");
        writer.println("</div>");
    }

    private static void renderRow(PrintWriter writer, Limit limit, int price, long maxVol, String type) {
        String rowClass = type.equals("ask") ? "ask-row" : "bid-row";
        String fillClass = type.equals("ask") ? "ask-fill" : "bid-fill";
        int vol = limit.getTotalVolume();
        int count = limit.getOrderCount();
        
        // Calculate percentage for bar, min 1% visibility
        int percentage = (int) ((double) vol / maxVol * 100);
        percentage = Math.max(1, percentage);

        writer.println("<tr class='" + rowClass + "'>");
        writer.println("<td>" + count + "</td>");
        writer.println("<td>" + vol + 
            "<div class='bar-container'><div class='" + fillClass + "' style='width:" + percentage + "%'></div></div>" +
            "</td>");
        writer.println("<td>" + price + "</td>");
        writer.println("</tr>");
    }
}
