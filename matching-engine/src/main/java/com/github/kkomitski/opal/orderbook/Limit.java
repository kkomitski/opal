package com.github.kkomitski.opal.orderbook;

/*
Chains a bunch of Limit chunks and manages them, effectively
creating a dynamic linked list, but it uses objects from a pre-reserved pool
*/
public class Limit {
  public static final int DEFAULT_MAX_CHUNKS_PER_LIMIT = 50;
  private final int maxChunksPerLevel;
  private final LimitChunkPool limitPool;
  private LimitChunk head;
  private LimitChunk tail;
  private int totalVolume = 0;
  private int orderCount = 0;
  private int chunksInChain = 0;

  public boolean initialized = false;

  public static enum State {
    NORMAL, FULL
  }

  public State state = State.NORMAL;

  public Limit(LimitChunkPool limitPool) {
    this(limitPool, DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public Limit(LimitChunkPool limitPool, int maxChunksPerLevel) {
    this.limitPool = limitPool;
    this.maxChunksPerLevel = maxChunksPerLevel;
    this.head = null;
    this.tail = null;
  }

  /**
   * Adds an order to the chain. Returns true if successful, false if pool
   * exhausted.
   */
  public boolean addOrder(OrderRequest req) {
    // initialize the head and tail if first addition or after reset
    if (head == null || tail == null) {
      head = tail = limitPool.getChunk();
      chunksInChain = 1;
    }

    int size = req.getQuantity();
    if (tail.addOrder(req)) {
      totalVolume += size;
      orderCount++;
      return true;
    } else {
      // Enforce maxChunksPerLevel
      if (chunksInChain >= maxChunksPerLevel) {
        state = State.FULL;
        return false;
      }
      // Tail chunk is full, need to chain a new chunk
      LimitChunk newChunk = limitPool.getChunk();
      chunksInChain++;
      tail.next = newChunk;
      tail = newChunk;
      if (tail.addOrder(req)) {
        orderCount++;
        totalVolume += size;
        return true;
      } else {
        // Should not happen unless pool is exhausted
        return false;
      }
    }
  }

  /**
   * Removes and returns the oldest order in the chain, or null if empty.
   */
  public Order removeOrder() {
    while (head != null) {
      Order order = head.removeOrder();
      if (order != null) {
        orderCount--;
        totalVolume -= order.size;
        // If head chunk emptied and chained, advance and release
        if (head.isEmpty() && head.next != null) {
          LimitChunk oldHead = head;
          head = head.next;
          oldHead.next = null;
          limitPool.releaseChunk(oldHead);
          chunksInChain--;
        }
        return order;
      }
      // No order in head; if chained, advance, else stop
      if (head.isEmpty() && head.next != null) {
        LimitChunk oldHead = head;
        head = head.next;
        oldHead.next = null;
        limitPool.releaseChunk(oldHead);
        chunksInChain--;
      } else {
        break;
      }
    }
    return null;
  }

  /**
   * Returns the oldest order without removing it, or null if empty.
   */
  public Order peek() {
    LimitChunk current = head;
    while (current != null) {
      Order order = current.peek();
      if (order != null && order.initialized) {
        return order;
      }
      current = current.next;
    }
    return null;
  }

  /**
   * Amends the size of the head order (oldest order) to the new size.
   */
  public void partialFill(int newSize) {
    if (head != null) {
      Order headOrder = head.peek();
      if (headOrder != null && headOrder.initialized) {
        int oldSize = headOrder.size;
        head.partialFill(newSize);
        totalVolume -= (oldSize - newSize);
      }
    }
  }

  public int getTotalVolume() {
    return totalVolume;
  }

  public int getOrderCount() {
    return orderCount;
  }

  public boolean isEmpty() {
    return totalVolume == 0;
  }

  public void reset() {
    // This is not very cache friendly...
    LimitChunk current = head;
    while (current != null) {
      limitPool.releaseChunk(current);
      LimitChunk next = current.next;
      orderCount = 0;
      current.next = null;
      current = next;
    }

    totalVolume = 0;
    initialized = false;
    head = null;
    tail = null;
    chunksInChain = 0;
    state = State.NORMAL;
  }
}
