/* 
TODO:
 - implement a tick size (decimal precision)
 - set the price collar (MAX_LIMITS_PER_BOOK) to a percentage of the price and tick size
*/

package com.github.kkomitski.opal;

import java.util.concurrent.Executors;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.helpers.OrderBookDump;
import com.github.kkomitski.opal.orderbook.Limit;
import com.github.kkomitski.opal.orderbook.LimitPool;
import com.github.kkomitski.opal.orderbook.Order;
import com.github.kkomitski.opal.orderbook.OrderRequest;
import com.github.kkomitski.opal.services.EgressService;
import com.github.kkomitski.opal.utils.MatchEventDecoder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.IntComparators;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

public class OrderBook {
  private final EpochClock epochClock;

  // Constants
  private final int MAX_LIMITS_PER_BOOK; // Price collar
  private static final int MAX_ORDERS_PER_CHUNK = 256;

  // Metadata
  private final String name;
  private final int instrumentIndex;

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

  // Reusable buffers for order request processing
  private OrderRequest orderRequestBuffer = new OrderRequest();
  private final byte[] matchEventBytes = new byte[MatchEventDecoder.SIZE];
  private final UnsafeBuffer matchEventBuffer = new UnsafeBuffer(matchEventBytes);

  private static final int ticksToRender = 50_000;

  public OrderBook(String name, int instrumentIndex, int limitsPerBook, int ordersPerLimit,
      EgressService egressService,
      EpochClock epochClock) {
    this.instrumentIndex = instrumentIndex;
    this.name = name;
    this.MAX_LIMITS_PER_BOOK = limitsPerBook;
    this.egressService = egressService;
    this.epochClock = epochClock;

    int chunksPerLimit = (int) Math.ceil((double) ordersPerLimit / MAX_ORDERS_PER_CHUNK);
    int chunkPoolSize = MAX_LIMITS_PER_BOOK * chunksPerLimit;

    limitPool = new LimitPool(MAX_LIMITS_PER_BOOK, chunkPoolSize, chunksPerLimit);

    this.bidLimits = new Int2ObjectHashMap<>(MAX_LIMITS_PER_BOOK / 2, 0.7f);
    this.askLimits = new Int2ObjectHashMap<>(MAX_LIMITS_PER_BOOK / 2, 0.7f);

    // Preallocate the heaps
    this.bidPrices = new IntHeapPriorityQueue(MAX_LIMITS_PER_BOOK / 2, IntComparators.OPPOSITE_COMPARATOR);
    this.askPrices = new IntHeapPriorityQueue(MAX_LIMITS_PER_BOOK / 2);

    this.disruptor = new Disruptor<OrderRequest>(
        OrderRequest::new,
        RING_BUFFER_SIZE,
        Executors.defaultThreadFactory(),
        ProducerType.MULTI,
        new BlockingWaitStrategy());

    // Process orders
    this.disruptor.handleEventsWith((order, sequence, endOfBatch) -> {
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

      OrderBookDump.generateHtml(this, "orderbook-dump.html");
      if (sequence % ticksToRender == 0) {
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

  private int MatchOrder(final OrderRequest takerOrder, int remainingSize, int price, int orderId, boolean isBid,
      boolean isLimit) {
    // Select the opposing book
    IntHeapPriorityQueue oppositePrices = isBid ? askPrices : bidPrices;
    Int2ObjectHashMap<Limit> oppositeLimits = isBid ? askLimits : bidLimits;

    final boolean shouldEmit = takerOrder != null;

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

        int matchPrice = isBid ? bestOppositePrice : bestOppositePrice;

        if (headOrder == null)
          break;

        if (headOrder.size == remainingSize) {
          // Complete fill
          Order matchedOrder = bestOppositeLimit.removeOrder();
          if (shouldEmit) {
            MatchEventDecoder.encode(orderId,
                matchedOrder.id,
                matchPrice,
                matchedOrder.size,
                epochClock.time(),
                matchEventBuffer,
                0);

            egressService.egress(matchEventBuffer, 0, MatchEventDecoder.SIZE);
          }
          remainingSize = 0;
        } else if (headOrder.size > remainingSize) {
          // Partial fill (more supply/demand left on opposite side)
          if (shouldEmit) {
            MatchEventDecoder.encode(orderId,
                headOrder.id,
                matchPrice,
                remainingSize,
                epochClock.time(),
                matchEventBuffer,
                0);

            egressService.egress(matchEventBuffer, 0, MatchEventDecoder.SIZE);
          }

          bestOppositeLimit.partialFill(remainingSize);
          remainingSize = 0;
        } else { // headOrder.size < remainingSize
          Order matchedOrder = bestOppositeLimit.removeOrder();

          if (shouldEmit) {
            MatchEventDecoder.encode(orderId,
                matchedOrder.id,
                matchPrice,
                matchedOrder.size,
                epochClock.time(),
                matchEventBuffer,
                0);
            egressService.egress(matchEventBuffer, 0, MatchEventDecoder.SIZE);
          }

          remainingSize -= headOrder.size;
        }
      }

      // Free up the limit entirely, remove price from queue
      if (bestOppositeLimit.getTotalVolume() == 0) {
        oppositePrices.dequeueInt();
        oppositeLimits.remove(bestOppositePrice);
        limitPool.releaseLimit(bestOppositeLimit);
      }
    }

    return remainingSize;
  }

  // TODO: This can be improved
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
    final Int2ObjectHashMap<Limit>.EntryIterator bidIt = bidLimits.entrySet().iterator();
    while (bidIt.hasNext()) {
      bidIt.next();
      final int price = bidIt.getIntKey();
      if (price < collarMin || price > collarMax) {
        final Limit limit = bidIt.getValue();
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
    final Int2ObjectHashMap<Limit>.EntryIterator askIt = askLimits.entrySet().iterator();
    while (askIt.hasNext()) {
      askIt.next();
      final int price = askIt.getIntKey();
      if (price < collarMin || price > collarMax) {
        final Limit limit = askIt.getValue();
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