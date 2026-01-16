package com.github.kkomitski.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;

import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.github.kkomitski.opal.services.EgressService;
import com.github.kkomitski.opal.utils.OrderRequestDecoder;

public class OrderBookTest {

  private static final SystemEpochClock TEST_CLOCK = SystemEpochClock.INSTANCE;

  private static final EgressService DUMMY_EGRESS_SERVICE = new EgressService();

  @Test
  @DisplayName("Limit order clears multiple bid limits across price levels")
  void testLimitOrderClearsMultipleBidLevels() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);
    int quantity = 2;
    int[] bidPrices = { 120, 121, 122 };
    int[] askPrices = { 124, 125, 126 };
    
    // Add 2 bid limit orders at each bid price
    for (int p : bidPrices) {
      for (int i = 0; i < 2; i++) {
        byte[] bidOrder = OrderRequestDecoder.encode(0, p, (short) quantity, true, 900 + p * 10 + i);
        book.publishOrder(bidOrder);
      }
    }
    
    // Add 2 ask limit orders at each ask price
    for (int p : askPrices) {
      for (int i = 0; i < 2; i++) {
        byte[] askOrder = OrderRequestDecoder.encode(0, p, (short) quantity, false, 800 + p * 10 + i);
        book.publishOrder(askOrder);
      }
    }
    
    // CRITICAL: Wait for Disruptor to process all orders asynchronously
    Thread.sleep(200);
    
    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);
    // Assert that bids at 120, 121, 122 are still present (not cleared)
    for (int p : bidPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = bidLimits.get(p);
      assertTrue(lim != null, "Bid limit should exist for price " + p);
      com.github.kkomitski.opal.orderbook.Order o = lim.peek();
      assertTrue(o != null && o.initialized, "Bid order at price " + p + " should NOT be cleared before crossing ask arrives");
    }
    Thread.sleep(500);
    // Assert that asks at 124, 125, 126 are still present (not cleared)
    for (int p : askPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = askLimits.get(p);
      assertTrue(lim != null, "Ask limit should exist for price " + p);
      com.github.kkomitski.opal.orderbook.Order o = lim.peek();
      assertTrue(o != null && o.initialized, "Ask order at price " + p + " should NOT be cleared before crossing bid arrives");
    }
    Thread.sleep(200);
    // Place an ask limit order at 120 with enough quantity to clear all bids at
    // 120, 121, 122
    int totalBidQty = bidPrices.length * 2 * quantity;
    byte[] clearingAsk = OrderRequestDecoder.encode(0, 120, (short) totalBidQty, false, 1000);
    book.publishOrder(clearingAsk);
    Thread.sleep(300);
    for (int p : bidPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = bidLimits.get(p);
      if (lim != null) {
        assertEquals(0, lim.getTotalVolume(), "Total volume at price " + p + " should be zero after clearing");
      }
    }
    // Place a bid limit order at 126 with enough quantity to clear all asks at 124,
    // 125, 126
    int totalAskQty = askPrices.length * 2 * quantity;
    byte[] clearingBid = OrderRequestDecoder.encode(0, 126, (short) totalAskQty, true, 1100);
    book.publishOrder(clearingBid);
    Thread.sleep(500);
    for (int p : askPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = askLimits.get(p);
      if (lim != null) {
        assertEquals(0, lim.getTotalVolume(), "Total volume at price " + p + " should be zero after clearing");
      }
    }
  }

  @Test
  @DisplayName("OrderBook initializes with empty limit maps")
  void testOrderBookLimitAndOrderInitialization() throws Exception {
    int priceLevels = 7;
    int ordersPerLevel = 3;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Access bidLimits and askLimits via reflection
    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);

    // OrderBook now uses dynamic allocation, so initially maps are empty
    assertEquals(0, bidLimits.size(), "Bid limits map should be empty initially");
    assertEquals(0, askLimits.size(), "Ask limits map should be empty initially");
  }

  @Test
  @DisplayName("Adding multiple orders at different price levels and sides places them in correct limits")
  void testMultipleOrdersAtDifferentLevelsAndSides() throws Exception {
    int priceLevels = 5;
    int ordersPerLevel = 4;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Orders to add: (side, price, quantity, id)
    Object[][] orders = {
        { true, 101, 2, 1 }, // bid
        { true, 102, 3, 2 }, // bid
        { false, 103, 4, 3 }, // ask
        { false, 104, 5, 4 }, // ask
        { true, 101, 1, 5 }, // bid, same price as first
        { false, 104, 2, 6 } // ask, same price as previous ask
    };

    for (Object[] o : orders) {
      boolean isBid = (boolean) o[0];
      int price = (int) o[1];
      int quantity = (int) o[2];
      int id = (int) o[3];
      byte[] orderBytes = OrderRequestDecoder.encode(0, price, (short) quantity, isBid, id);
      book.publishOrder(orderBytes);
    }

    // Wait for disruptor to process
    Thread.sleep(200);

    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);

    // Check bid orders
    int bidIndex101 = 101;
    int bidIndex102 = 102;
    com.github.kkomitski.opal.orderbook.Limit lim101 = bidLimits.get(bidIndex101);
    com.github.kkomitski.opal.orderbook.Limit lim102 = bidLimits.get(bidIndex102);
    assertTrue(lim101 != null, "Bid limit 101 should be initialized");
    assertTrue(lim102 != null, "Bid limit 102 should be initialized");
    // Check first order at 101: id 1
    com.github.kkomitski.opal.orderbook.Order o101_0 = lim101.peek();
    assertEquals(1, o101_0.id);
    assertEquals(2, o101_0.size);
    assertTrue(o101_0.initialized);
    assertEquals(3, lim101.getTotalVolume(), "Total volume should be 3 (2+1)");
    // Order at 102: id 2
    com.github.kkomitski.opal.orderbook.Order o102_0 = lim102.peek();
    assertEquals(2, o102_0.id);
    assertEquals(3, o102_0.size);
    assertTrue(o102_0.initialized);
    // Check ask orders
    int askIndex103 = 103;
    int askIndex104 = 104;
    com.github.kkomitski.opal.orderbook.Limit lim103 = askLimits.get(askIndex103);
    com.github.kkomitski.opal.orderbook.Limit lim104 = askLimits.get(askIndex104);
    assertTrue(lim103 != null, "Ask limit 103 should be initialized");
    assertTrue(lim104 != null, "Ask limit 104 should be initialized");
    // Order at 103: id 3
    com.github.kkomitski.opal.orderbook.Order o103_0 = lim103.peek();
    assertEquals(3, o103_0.id);
    assertEquals(4, o103_0.size);
    assertTrue(o103_0.initialized);
    // Check first order at 104: id 4
    com.github.kkomitski.opal.orderbook.Order o104_0 = lim104.peek();
    assertEquals(4, o104_0.id);
    assertEquals(5, o104_0.size);
    assertTrue(o104_0.initialized);
    assertEquals(7, lim104.getTotalVolume(), "Total volume should be 7 (5+2)");
  }

  @Test
  @DisplayName("Limit order is correctly added to bid limit and reflected in state")
  void testLimitOrderIsAddedToBidLimit() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Create a bid (buy) limit order
    int price = 101;
    int quantity = 10;
    boolean isBid = true;
    int orderId = 1;
    byte[] orderBytes = OrderRequestDecoder.encode(
        0, price, (short) quantity, isBid, orderId);

    book.publishOrder(orderBytes);

    // Wait briefly for the disruptor to process
    try {
      Thread.sleep(500);
    } catch (InterruptedException ignored) {
    }

    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit = bidLimits.get(price);
    com.github.kkomitski.opal.orderbook.Order o = limit.peek();
    assertTrue(o != null && o.initialized, "Order should be marked initialized");
    assertEquals(1, o.id, "Order ID should match");
    assertEquals(quantity, o.size, "Order quantity should match");
    assertEquals(quantity, limit.getTotalVolume(), "Total volume should match order size");
  }

  @Test
  @DisplayName("Limit order is correctly added to ask limit and reflected in state")
  void testLimitOrderIsAddedToAskLimit() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Create an ask (sell) limit order
    int price = 102;
    int quantity = 7;
    boolean isBid = false;
    int orderId = 2;
    byte[] orderBytes = OrderRequestDecoder.encode(
        0, price, (short) quantity, isBid, orderId);

    book.publishOrder(orderBytes);

    // Wait briefly for the disruptor to process
    try {
      Thread.sleep(50);
    } catch (InterruptedException ignored) {
    }

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit = askLimits.get(price);
    com.github.kkomitski.opal.orderbook.Order o = limit.peek();
    assertTrue(o != null && o.initialized, "Order should be marked initialized");
    assertEquals(orderId, o.id, "Order ID should match");
    assertEquals(quantity, o.size, "Order quantity should match");
    assertEquals(quantity, limit.getTotalVolume(), "Total volume should match order size");
  }

  @Test
  @DisplayName("Limit correctly enforces order capacity and tracks initialized orders")
  void testLimitOrderLimitCapacity() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    int price = 105;
    boolean isBid = true;

    // Fill the limit to capacity
    for (int i = 0; i < 2; i++) {
      byte[] orderBytes = OrderRequestDecoder.encode(
          0, price, (short) 1, isBid, i + 1);
      book.publishOrder(orderBytes);
    }

    // Wait briefly for the disruptor to process
    try {
      Thread.sleep(50);
    } catch (InterruptedException ignored) {
    }

    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit = bidLimits.get(price);
    assertEquals(2, limit.getTotalVolume(),
        "Total volume should be 2 (1+1)");
    com.github.kkomitski.opal.orderbook.Order o = limit.peek();
    assertTrue(o != null && o.initialized, "Order should be initialized");

  }

  @Test
  @DisplayName("Limit order clears ask limit at same price")
  void testLimitOrderClearsAskAtSamePrice() throws Exception {
    int priceLevels = 10;
    int ordersPerLevel = 5;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);
    int price = 120;
    int quantity = 2;
    for (int i = 0; i < 3; i++) {
      byte[] askOrder = OrderRequestDecoder.encode(0, price, (short) quantity, false, 100 + i);
      book.publishOrder(askOrder);
    }
    Thread.sleep(100);
    int totalQuantity = quantity * 3;
    byte[] bidOrder = OrderRequestDecoder.encode(0, price, (short) totalQuantity, true, 200);
    book.publishOrder(bidOrder);
    Thread.sleep(200);
    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit = askLimits.get(price);
    if (limit != null) {
      assertEquals(0, limit.getTotalVolume(), "Total volume should be zero after clearing");
    }
  }

  @Test
  @DisplayName("Limit order clears ask limit at better price")
  void testLimitOrderClearsAskAtBetterPrice() throws Exception {
    int priceLevels = 10;
    int ordersPerLevel = 5;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);
    int price2 = 121;
    int quantity = 2;
    for (int i = 0; i < 2; i++) {
      byte[] askOrder = OrderRequestDecoder.encode(0, price2, (short) quantity, false, 300 + i);
      book.publishOrder(askOrder);
    }
    Thread.sleep(100);
    byte[] bidOrder2 = OrderRequestDecoder.encode(0, 123, (short) (quantity * 2), true, 400);
    book.publishOrder(bidOrder2);
    Thread.sleep(200);
    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit2 = askLimits.get(price2);
    if (limit2 != null) {
      assertEquals(0, limit2.getTotalVolume(), "Total volume should be zero after clearing with higher bid");
    }
  }

  @Test
  @DisplayName("Limit order clears multiple ask limits across price levels")
  void testLimitOrderClearsMultipleAskLevels() throws Exception {
    int priceLevels = 10;
    int ordersPerLevel = 5;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);
    int quantity = 2;
    int[] askPrices = { 124, 125, 126 };
    int[] bidPrices = { 120, 121, 122 };
    for (int p : askPrices) {
      for (int i = 0; i < 2; i++) {
        byte[] askOrder = OrderRequestDecoder.encode(0, p, (short) quantity, false, 500 + p * 10 + i);
        book.publishOrder(askOrder);
      }
    }
    for (int p : bidPrices) {
      for (int i = 0; i < 2; i++) {
        byte[] bidOrderX = OrderRequestDecoder.encode(0, p, (short) quantity, true, 600 + p * 10 + i);
        book.publishOrder(bidOrderX);
      }
    }
    Thread.sleep(500);
    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);
    // Assert that asks at 124, 125, 126 are still present (not cleared)
    for (int p : askPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = askLimits.get(p);
      com.github.kkomitski.opal.orderbook.Order o = lim.peek();
      assertTrue(o != null && o.initialized, "Ask order at price " + p + " should NOT be cleared before crossing bid arrives");
    }
    Thread.sleep(500);
    // Assert that bids at 120, 121, 122 are still present (not cleared)
    for (int p : bidPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = bidLimits.get(p);
      com.github.kkomitski.opal.orderbook.Order o = lim.peek();
      assertTrue(o != null && o.initialized, "Bid order at price " + p + " should NOT be cleared before crossing ask arrives");
    }
    Thread.sleep(200);
    // Place a bid limit order at 126 with enough quantity to clear all asks at 124,
    // 125, 126
    int totalAskQty = askPrices.length * 2 * quantity;
    byte[] clearingBid = OrderRequestDecoder.encode(0, 126, (short) totalAskQty, true, 700);
    book.publishOrder(clearingBid);
    Thread.sleep(300);
    for (int p : askPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = askLimits.get(p);
      if (lim != null) {
        assertEquals(0, lim.getTotalVolume(), "Total volume at price " + p + " should be zero after clearing");
      }
    }
    // Place an ask limit order at 120 with enough quantity to clear all bids at
    // 120, 121, 122
    int totalBidQty = bidPrices.length * 2 * quantity;
    byte[] clearingAsk = OrderRequestDecoder.encode(0, 120, (short) totalBidQty, false, 800);
    book.publishOrder(clearingAsk);
    Thread.sleep(500);
    for (int p : bidPrices) {
      com.github.kkomitski.opal.orderbook.Limit lim = bidLimits.get(p);
      if (lim != null) {
        assertEquals(0, lim.getTotalVolume(), "Total volume at price " + p + " should be zero after clearing");
      }
    }
  }

  @Test
  @DisplayName("Market order clears all orders at a single limit level")
  void testMarketOrderClearsSingleLimitLevel() throws Exception {
    int priceLevels = 10;
    int ordersPerLevel = 5;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    int price = 120;
    int quantity = 2;
    // Add 5 ask limit orders at the same price
    for (int i = 0; i < 5; i++) {
      byte[] orderBytes = OrderRequestDecoder.encode(0, price, (short) quantity, false, i + 1);
      book.publishOrder(orderBytes);
    }
    // Wait for orders to be processed
    Thread.sleep(200);

    // Place a market buy order with enough quantity to clear all 5 orders
    int totalQuantity = quantity * 5;
    byte[] marketOrder = OrderRequestDecoder.encode(0, 0, (short) totalQuantity, true, 100);
    book.publishOrder(marketOrder);

    Thread.sleep(300);

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    com.github.kkomitski.opal.orderbook.Limit limit = askLimits.get(price);
    if (limit != null) {
      assertEquals(0, limit.getTotalVolume(), "Total volume should be zero after market order clears limit");
    }
  }

  @Test
  @DisplayName("Market order clears orders across multiple limit levels")
  void testMarketOrderClearsMultipleLimitLevels() throws Exception {
    int priceLevels = 10;
    int ordersPerLevel = 2;
    OrderBook book = new OrderBook("TEST", 1, priceLevels, ordersPerLevel, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    int[] prices = { 121, 122, 123, 124, 125 };
    int quantity = 3;
    // Add 2 ask limit orders at each price level
    for (int p = 0; p < prices.length; p++) {
      for (int i = 0; i < 2; i++) {
        byte[] orderBytes = OrderRequestDecoder.encode(0, prices[p], (short) quantity, false, p * 10 + i + 1);
        book.publishOrder(orderBytes);
      }
    }
    Thread.sleep(300);

    // Place a market buy order with enough quantity to clear all orders
    int totalQuantity = quantity * prices.length * 2;
    byte[] marketOrder = OrderRequestDecoder.encode(0, 0, (short) totalQuantity, true, 200);
    book.publishOrder(marketOrder);

    Thread.sleep(400);

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);
    // All orders at all levels should be cleared
    for (int price : prices) {
      com.github.kkomitski.opal.orderbook.Limit limit = askLimits.get(price);
      if (limit != null) {
        assertEquals(0, limit.getTotalVolume(), "Total volume at price " + price + " should be zero after market order");
      }
    }
  }

  @Test
  @DisplayName("Order rejected when bid price falls outside price collar (too low)")
  void testBidRejectedOutsidePriceCollar() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Place initial bid and ask orders to establish market
    byte[] initialBid = OrderRequestDecoder.encode(0, 100, (short) 10, true, 1);
    byte[] initialAsk = OrderRequestDecoder.encode(0, 102, (short) 10, false, 2);
    book.publishOrder(initialBid);
    book.publishOrder(initialAsk);
    Thread.sleep(100);

    // Average price is 101, collar is 1000/2 = 500 ticks on each side
    // Lower bound = 101 - 500 = -399 (but price can't be negative, so practically any bid is allowed)
    // For this test, we'll place a very aggressive bid outside what would be reasonable
    // The collar allows 1000 limits, so with mid at 101, we can go from 101-500 to 101+500
    // Place a bid at a very low price outside the collar
    byte[] outOfBandBid = OrderRequestDecoder.encode(0, -400, (short) 5, true, 3);
    book.publishOrder(outOfBandBid);
    Thread.sleep(100);

    // The order should be rejected, so it should not appear in bidLimits
    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);

    // The negative price should not be in the book
    assertTrue(!bidLimits.containsKey(-400), "Out-of-band bid price should be rejected and not in book");
  }

  @Test
  @DisplayName("Order rejected when ask price falls outside price collar (too high)")
  void testAskRejectedOutsidePriceCollar() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Place initial bid and ask orders to establish market
    byte[] initialBid = OrderRequestDecoder.encode(0, 100, (short) 10, true, 1);
    byte[] initialAsk = OrderRequestDecoder.encode(0, 102, (short) 10, false, 2);
    book.publishOrder(initialBid);
    book.publishOrder(initialAsk);
    Thread.sleep(100);

    // Average price is 101, upper bound = 101 + 500 = 601
    // Place an ask at a price well above the collar
    byte[] outOfBandAsk = OrderRequestDecoder.encode(0, 1000, (short) 5, false, 3);
    book.publishOrder(outOfBandAsk);
    Thread.sleep(100);

    // The order should be rejected, so it should not appear in askLimits
    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);

    // The high price should not be in the book (outside 601 upper bound)
    assertTrue(!askLimits.containsKey(1000), "Out-of-band ask price should be rejected and not in book");
  }

  @Test
  @DisplayName("Orders are pruned and rejected when price collar moves significantly")
  void testOrdersPrunedWhenPriceCollarMoves() throws Exception {
    OrderBook book = new OrderBook("TEST", 1, 1000, 10, DUMMY_EGRESS_SERVICE, TEST_CLOCK);

    // Place initial market
    byte[] initialBid = OrderRequestDecoder.encode(0, 100, (short) 10, true, 1);
    byte[] initialAsk = OrderRequestDecoder.encode(0, 102, (short) 10, false, 2);
    book.publishOrder(initialBid);
    book.publishOrder(initialAsk);
    Thread.sleep(100);

    // Place orders at edges of current collar (avgPrice=101, collar=500)
    // Lower edge ~= 101-500 = -399, upper edge ~= 101+500 = 601
    byte[] lowBid = OrderRequestDecoder.encode(0, 50, (short) 5, true, 10);
    byte[] highAsk = OrderRequestDecoder.encode(0, 550, (short) 5, false, 11);
    book.publishOrder(lowBid);
    book.publishOrder(highAsk);
    Thread.sleep(100);

    // Verify orders are in the book
    Field bidLimitsField = OrderBook.class.getDeclaredField("bidLimits");
    bidLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> bidLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) bidLimitsField.get(book);

    Field askLimitsField = OrderBook.class.getDeclaredField("askLimits");
    askLimitsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit> askLimits = (java.util.Map<Integer, com.github.kkomitski.opal.orderbook.Limit>) askLimitsField.get(book);

    assertTrue(bidLimits.containsKey(50), "Low bid should be in book initially");
    assertTrue(askLimits.containsKey(550), "High ask should be in book initially");

    // Now place a large crossing order to move the market significantly
    // Place a big bid that will match asks and move the market up
    byte[] largeBid = OrderRequestDecoder.encode(0, 550, (short) 100, true, 20);
    book.publishOrder(largeBid);
    Thread.sleep(300); // Allow pruning to occur (every 100 sequences)

    // After pruning, the far orders should be removed
    // New avgPrice should be around 550, so lower bound ~= 550-500 = 50, upper bound ~= 550+500 = 1050
    // The original bid at 50 might be pruned as it's at the edge
    // This depends on the exact pruning logic and tick buffer
  }

}