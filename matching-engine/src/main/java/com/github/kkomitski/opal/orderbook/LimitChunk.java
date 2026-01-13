package com.github.kkomitski.opal.orderbook;

/*
FIFO style limit - oldest orders get filled first
*/
public class LimitChunk {
  // Defaults
  private static final int DEFAULT_CHUNK_SIZE = 256;

  // Header
  public boolean initialized;
  public LimitChunk next;
  private int head = 0;
  private int tail = 0;
  private int capacity;
  private int count = 0;
  // TODO: Maybe introduce a state machine enum to track ACTIVE, INACTIVE, FULL, ERROR?

  // Data
  public final Order[] orders;
  public int chunkVolume = 0;

  public LimitChunk() {
    this(DEFAULT_CHUNK_SIZE);
  }

  public LimitChunk(int ordersPerChunk) {
    capacity = ordersPerChunk;
    orders = new Order[capacity];

    for (int i = 0; i < ordersPerChunk; i++) {
      orders[i] = new Order();
    }
  }

  public boolean addOrder(OrderRequest orderRequest) {
    if (count < capacity) {
      orders[tail].size = orderRequest.getQuantity();
      orders[tail].id = orderRequest.getId();
      orders[tail].initialized = true;
      chunkVolume += orders[tail].size;
      tail = (short) ((tail + 1) % DEFAULT_CHUNK_SIZE);
      count++;

      return true;
    }

    return false; // Limit is full
  }

  public Order removeOrder() {
    if (count > 0) {
      Order order = orders[head];
      orders[head].initialized = false;
      head = (short) ((head + 1) % DEFAULT_CHUNK_SIZE);
      count--;
      chunkVolume -= order.size;
      return order;
    }

    this.initialized = false; // self de-init
    return null; // Limit is empty
  }

  // Amends the size of the head order
  public void partialFill(int newSize) {
    if (count > 0) {
      int oldSize = orders[head].size;
      orders[head].size = newSize;
      chunkVolume -= (oldSize - newSize);
    }
  }

  public Order peek() {
    if (count > 0) {
      return orders[head];
    }

    return null;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public void reset() {
    // TODO: Check how this negotiates with the orderbook pruning
    // Clear only the active orders (from head, count times)
    int idx = head;
    for (int i = 0; i < count; i++) {
      orders[idx].initialized = false;
      orders[idx].size = 0;
      orders[idx].id = 0;
      idx = (short) ((idx + 1) % DEFAULT_CHUNK_SIZE);
    }
    chunkVolume = 0;
    capacity = DEFAULT_CHUNK_SIZE;
    count = 0;
    tail = 0;
    head = 0;
    initialized = false;
  }
}
