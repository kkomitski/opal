/* 
TODO:
 - implement a tick size (decimal precision)
 - set the price collar (MAX_LIMITS_PER_BOOK) to a percentage of the price and tick size
*/

package com.github.kkomitski.opal;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.orderbook.Limit;
import com.github.kkomitski.opal.orderbook.LimitPool;
import com.github.kkomitski.opal.orderbook.Order;
import com.github.kkomitski.opal.orderbook.OrderRequest;
import com.github.kkomitski.opal.services.EgressService;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.IntComparators;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

public class OrderBook {
  // Constants
  private final int MAX_LIMITS_PER_BOOK; // Price collar
  private static final int MAX_ORDERS_PER_CHUNK = 256;

  // Metadata
  private final String name;
  private final int instrumentIndex;
  private boolean warned75 = false;
  private boolean warned85 = false;
  private boolean crashed95 = false;

  private final Disruptor<OrderRequest> disruptor;
  private final RingBuffer<OrderRequest> ringBuffer;
  private final int RING_BUFFER_SIZE = 32_768; // Increased from 8132

  private final Int2ObjectHashMap<Limit> bidLimits;
  private final Int2ObjectHashMap<Limit> askLimits;

  private final IntHeapPriorityQueue bidPrices; // Max-heap
  private final IntHeapPriorityQueue askPrices; // Min-heap

  // 1000 chunks * 256 orders = 256,000 orders total across 300 levels
  private final LimitPool limitPool;

  // Used to throw messages out of the orderbook to via IPC
  private final EgressService egressService;

  // Diagnostics
  private double disruptorUsage = 0;

  // private DirectBuffer orderToRequestBuffer = new UnsafeBuffer(new
  // byte[OrderRequest.REQUEST_SIZE]);
  private OrderRequest orderRequestBuffer = new OrderRequest();

  private static final int ticksToRender = 50_000;

  public OrderBook(String name, int instrumentIndex, int limitsPerBook, int ordersPerLimit, EgressService egressService) {
    this.instrumentIndex = instrumentIndex;
    this.name = name;
    this.MAX_LIMITS_PER_BOOK = limitsPerBook;
    this.egressService = egressService;

    int chunksPerLimit = (int) Math.ceil((double) ordersPerLimit / MAX_ORDERS_PER_CHUNK);
    int chunkPoolSize = MAX_LIMITS_PER_BOOK * chunksPerLimit;

    limitPool = new LimitPool(MAX_LIMITS_PER_BOOK, chunkPoolSize, chunksPerLimit);

    this.bidLimits = new Int2ObjectHashMap<>(MAX_LIMITS_PER_BOOK / 2, 0.7f);
    this.askLimits = new Int2ObjectHashMap<>(MAX_LIMITS_PER_BOOK / 2, 0.7f);

    // Preallocate the heaps
    this.bidPrices = new IntHeapPriorityQueue(MAX_LIMITS_PER_BOOK / 2, IntComparators.OPPOSITE_COMPARATOR);
    this.askPrices = new IntHeapPriorityQueue(MAX_LIMITS_PER_BOOK / 2);

    // this.disruptor = new Disruptor<OrderRequest>(
    // OrderRequest::new,
    // RING_BUFFER_SIZE,
    // Executors.defaultThreadFactory());
    this.disruptor = new Disruptor<OrderRequest>(
        OrderRequest::new,
        RING_BUFFER_SIZE,
        Executors.defaultThreadFactory(),
        ProducerType.MULTI,
        new BlockingWaitStrategy());

    // Add event handler to process orders
    this.disruptor.handleEventsWith((order, sequence, endOfBatch) -> {
      // checkBuffer(sequence);

      int price = order.getPrice();
      int quantity = order.getQuantity();
      boolean isMarket = price == 0;
      boolean isCancel = price == 0 && quantity == 0;

      if (isCancel) {
        this.CancelOrder(order);
      } else if (isMarket) {
        this.PlaceMarketOrder(order);
      } else {
        this.PlaceLimitOrder(order);
      }

      // Can run it every N sequence calls instead if we allow a buffer zone in the
      // levels
      if (sequence % 100 == 0) {
        pruneStaleLevels(100);
      }
      if (sequence % ticksToRender == 0) {
        // OrderBookDump.generateHtml(this, "orderbook-dump.html");
        // pruneStaleLevels(100);
      }
    });

    this.disruptor.start();
    this.ringBuffer = disruptor.getRingBuffer();
  }

  public void publishOrder(ByteBuf buf) {
    long sequence = ringBuffer.next();
    try {
      OrderRequest orderRequestSlot = ringBuffer.get(sequence);
      byte[] unboxedBytes = ((UnsafeBuffer) orderRequestSlot.buffer).byteArray();
      buf.getBytes(0, unboxedBytes, 0, OrderRequest.REQUEST_SIZE);
    } finally {
      ringBuffer.publish(sequence);
    }
  }

  public void publishOrder(DirectBuffer buffer, int offset) {
    long sequence = ringBuffer.next();
    try {
      OrderRequest orderRequestSlot = ringBuffer.get(sequence);
      byte[] unboxedBytes = ((UnsafeBuffer) orderRequestSlot.buffer).byteArray();
      buffer.getBytes(offset, unboxedBytes, 0, OrderRequest.REQUEST_SIZE);
    } finally {
      ringBuffer.publish(sequence);
    }
  }

  public void publishOrder(byte[] orderBytes) {
    long sequence = ringBuffer.next();
    try {
      OrderRequest event = ringBuffer.get(sequence);
      event.setBytes(orderBytes);
    } finally {
      ringBuffer.publish(sequence);
    }
  }

  public RingBuffer<OrderRequest> getRingBuffer() {
    return ringBuffer;
  }

  private void CancelOrder(OrderRequest order) {
  }

  private void RejectOrder(OrderRequest order, OrderRequest.RejectionReason reason) {
    int bestBid = bidPrices.isEmpty() ? -1 : bidPrices.firstInt();
    int bestAsk = askPrices.isEmpty() ? -1 : askPrices.firstInt();
    int avgPrice = (bestBid != -1 && bestAsk != -1) ? (bestBid + bestAsk) / 2 : -1;

    System.out.printf(
        "Order REJECTED [%s]: Price=%d, AvgPrice=%d, BestBid=%d, BestAsk=%d, Reason=%s, OrderID=%d%n",
        order.isBid() ? "BID" : "ASK",
        order.getPrice(),
        avgPrice,
        bestBid,
        bestAsk,
        reason,
        order.getId());
  }

  private void PlaceMarketOrder(OrderRequest order) {
    boolean isBid = order.isBid();
    int size = order.getQuantity();
    int orderId = order.getId();

    int remainingSize = MatchOrder(order, size, 0, orderId, isBid, false);

    if (remainingSize > 0) {
      // Maybe emit a message?
    }
  }

  private void PlaceLimitOrder(OrderRequest order) {
    boolean isBid = order.isBid();
    int orderId = order.getId();
    int orderPrice = order.getPrice();

    // Enforce price collar based on current book state
    int center = orderPrice; // Default to order price if no book exists
    if (!bidPrices.isEmpty()) {
      center = bidPrices.firstInt();
      if (!askPrices.isEmpty()) {
        center = (center + askPrices.firstInt()) / 2;
      }
    } else if (!askPrices.isEmpty()) {
      center = askPrices.firstInt();
    }

    int lowerBound = Math.max(1, center - (MAX_LIMITS_PER_BOOK / 2));
    int upperBound = center + (MAX_LIMITS_PER_BOOK / 2);

    // Check price collar
    if (isBid && orderPrice < lowerBound) {
      RejectOrder(order, OrderRequest.RejectionReason.BID_PRICE_TOO_LOW);
      return;
    }
    if (!isBid && orderPrice > upperBound) {
      RejectOrder(order, OrderRequest.RejectionReason.ASK_PRICE_TOO_HIGH);
      return;
    }

    if (isBid) { // (buy)
      // Reject bid orders that are too cheap - ie outside of the price collar
      int bidPrice = orderPrice;
      int size = order.getQuantity();
      int remainingSize = MatchOrder(order, size, bidPrice, orderId, true, true);

      // Still some remaining demand that cannot be met - add to the orderbook
      if (remainingSize > 0) {
        // Get or create the limit for this price
        boolean isNew = !bidLimits.containsKey(bidPrice);
        Limit bidLimit = bidLimits.computeIfAbsent(bidPrice, k -> limitPool.getLimit());

        if (isNew) { // add new prices to the priority queue
          bidPrices.enqueue(bidPrice);
        }

        boolean success = bidLimit.addOrder(order);

        if (!success) {
          if (bidLimit.state == Limit.State.FULL) {
            // Move the order into a limit in the spare hashmap (to be implemented..)
            System.out.printf("BID Limit for instrument %s '%d' is full!", name, bidPrice);
            RejectOrder(order, OrderRequest.RejectionReason.PRICE_LIMIT_FULL);
          }
        }
      }
    } else { // is ask (sell)
      int askPrice = orderPrice;
      int size = order.getQuantity();

      int remainingSize = MatchOrder(order, size, askPrice, orderId, false, true);

      // Still some remaining supply that cannot be met - add to the orderbook
      if (remainingSize > 0) {
        // Get or create the limit for this price
        boolean isNew = !askLimits.containsKey(askPrice);
        Limit askLimit = askLimits.computeIfAbsent(askPrice, k -> limitPool.getLimit());

        if (isNew) { // add new prices to the priority queue
          askPrices.enqueue(askPrice);
        }

        boolean success = askLimit.addOrder(order);

        if (!success) {
          System.out.printf("ASK Limit for instrument %s '%d' is full!", name, askPrice);
          RejectOrder(order, OrderRequest.RejectionReason.PRICE_LIMIT_FULL);
        }
      }
    }

  }

  private int MatchOrder(final OrderRequest takerOrder, int remainingSize, int price, int orderId, boolean isBid, boolean isLimit) {
    // Select the opposing book
    IntHeapPriorityQueue oppositePrices = isBid ? askPrices : bidPrices;
    Map<Integer, Limit> oppositeLimits = isBid ? askLimits : bidLimits;

    // Opaque egress payload (protocol TBD). For now we just ship the taker OrderRequest bytes.
    final DirectBuffer takerBuffer = takerOrder == null ? null : takerOrder.buffer;

    // While we still have demand/supply and opposing orders exist
    while (remainingSize > 0 && !oppositePrices.isEmpty()) {
      int bestOppositePrice = oppositePrices.firstInt();

      // Check if price is acceptable for limit orders
      if (isLimit) {
        if (isBid && bestOppositePrice > price)
          break; // Ask too expensive
        if (!isBid && bestOppositePrice < price)
          break; // Bid too low
      }

      // Get the limit by price
      Limit bestOppositeLimit = oppositeLimits.get(bestOppositePrice);
      if (bestOppositeLimit == null) {
        // Shouldn't happen if heaps are consistent, but safety check
        oppositePrices.dequeueInt();
        continue;
      }

      // Match against orders at this price level
      while (remainingSize > 0 && bestOppositeLimit.getTotalVolume() > 0) {
        Order headOrder = bestOppositeLimit.peek();
        if (headOrder == null)
          break;

        if (headOrder.size == remainingSize) {
          // Complete fill
          bestOppositeLimit.removeOrder();

          if (takerBuffer != null) {
            egressService.egress(takerBuffer, 0, OrderRequest.REQUEST_SIZE);
          }
          remainingSize = 0;
        } else if (headOrder.size > remainingSize) {
          // Partial fill (more supply/demand left on opposite side)
          if (takerBuffer != null) {
            egressService.egress(takerBuffer, 0, OrderRequest.REQUEST_SIZE);
          }

          bestOppositeLimit.partialFill(remainingSize);
          remainingSize = 0;
        } else { // headOrder.size < remainingSize
          bestOppositeLimit.removeOrder();

          if (takerBuffer != null) {
            egressService.egress(takerBuffer, 0, OrderRequest.REQUEST_SIZE);
          }

          remainingSize -= headOrder.size;
        }
      }

      // Free up the limit entirely, remove price from queue
      if (bestOppositeLimit.getTotalVolume() == 0) {
        oppositePrices.dequeueInt();
        oppositeLimits.remove(bestOppositePrice); // This could potentially trigger GC
        limitPool.releaseLimit(bestOppositeLimit);
      }
    }

    return remainingSize;
  }

  private void pruneStaleLevels(int ticksBuffer) {
    // Always prune based on the current valid collar, regardless of book state
    // Center collar around the midpoint of best bid/ask if available, else use
    // bestBid or bestAsk
    int center;
    if (!bidPrices.isEmpty() && !askPrices.isEmpty()) {
      center = (bidPrices.firstInt() + askPrices.firstInt()) / 2;
    } else if (!bidPrices.isEmpty()) {
      center = bidPrices.firstInt();
    } else if (!askPrices.isEmpty()) {
      center = askPrices.firstInt();
    } else {
      // No prices, nothing to prune
      return;
    }

    int halfBook = MAX_LIMITS_PER_BOOK / 2;
    int collarMin = Math.max(1, center - halfBook);
    int collarMax = center + halfBook;

    // Prune bids outside collar
    Iterator<Map.Entry<Integer, Limit>> bidIt = bidLimits.entrySet().iterator();
    while (bidIt.hasNext()) {
      Map.Entry<Integer, Limit> e = bidIt.next();
      int price = e.getKey();
      if (price < collarMin || price > collarMax) {
        Limit limit = e.getValue();
        while (!limit.isEmpty()) {
          Order order = limit.removeOrder();
          orderRequestBuffer.setFromOrder(order, orderRequestBuffer, true, price, instrumentIndex);
          RejectOrder(orderRequestBuffer, OrderRequest.RejectionReason.AVG_PRICE_MOVED_TOO_FAR);
        }
        bidIt.remove();
        if (limit != null) {
          limitPool.releaseLimit(limit);
        }
      }
    }

    // Prune asks outside collar
    Iterator<Map.Entry<Integer, Limit>> askIt = askLimits.entrySet().iterator();
    while (askIt.hasNext()) {
      Map.Entry<Integer, Limit> e = askIt.next();
      int price = e.getKey();
      if (price < collarMin || price > collarMax) {
        Limit limit = e.getValue();
        while (!limit.isEmpty()) {
          Order order = limit.removeOrder();
          orderRequestBuffer.setFromOrder(order, orderRequestBuffer, false, price, instrumentIndex);
          RejectOrder(orderRequestBuffer, OrderRequest.RejectionReason.AVG_PRICE_MOVED_TOO_FAR);
        }
        askIt.remove();
        if (limit != null) {
          limitPool.releaseLimit(limit);
        }
      }
    }

    // Clean up heaps: drop prices no longer in maps
    while (!bidPrices.isEmpty() && !bidLimits.containsKey(bidPrices.firstInt())) {
      bidPrices.dequeueInt();
    }
    while (!askPrices.isEmpty() && !askLimits.containsKey(askPrices.firstInt())) {
      askPrices.dequeueInt();
    }
  }

  public void shutdown() {
    disruptor.shutdown();
  }

  public void checkBuffer(long sequence) {
    if (sequence % 10_000 == 0) { // Check every 100 orders to reduce overhead
      long remaining = disruptor.getRingBuffer().remainingCapacity();
      long used = RING_BUFFER_SIZE - remaining;
      double usage = (double) used / RING_BUFFER_SIZE;

      disruptorUsage = usage;

      // OrderBookDump.generateHtml(this, "orderbook-dump.html");

      if (usage >= 0.95 && !crashed95) {
        System.err.println("\u001B[31mCRITICAL: Disruptor buffer over 95% full! Crashing...\u001B[0m");
        crashed95 = true;
        throw new IllegalStateException("Disruptor buffer over 95% full!");
      } else if (usage >= 0.85 && !warned85) {
        System.out.println("\u001B[33mWARNING: Disruptor buffer is over 85% full!\u001B[0m");
        warned85 = true;
      } else if (usage < 0.85 && warned85) {
        System.out.println("\u001B[32mINFO: Disruptor buffer usage dropped below 85%.\u001B[0m");
        warned85 = false;
        crashed95 = false;
      }

      if (usage >= 0.75 && !warned75) {
        System.out.println("\u001B[34mNOTICE: Disruptor buffer is over 75% full!\u001B[0m");
        warned75 = true;
      } else if (usage < 0.75 && warned75) {
        System.out.println("\u001B[36mINFO: Disruptor buffer usage dropped below 75%.\u001B[0m");
        warned75 = false;
      }
    }
  }

  // --- Getters for OrderBookDump ---

  public String getName() {
    return name;
  }

  public Int2ObjectHashMap<Limit> getBidLimits() {
    return bidLimits;
  }

  public Int2ObjectHashMap<Limit> getAskLimits() {
    return askLimits;
  }

  public double getDisruptorUsage() {
    return disruptorUsage;
  }

  public long getRingBufferCapacity() {
    return RING_BUFFER_SIZE;
  }

  public int getInstrumentIndex() {
    return instrumentIndex;
  }

  public int getBestBid() {
    return bidPrices.isEmpty() ? 0 : bidPrices.firstInt();
  }

  public int getBestAsk() {
    return askPrices.isEmpty() ? 0 : askPrices.firstInt();
  }

  public LimitPool getLimitPool() {
    return limitPool;
  }
}