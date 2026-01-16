package com.github.kkomitski.opal.services;


// Physical cores: 0 ─ 11

// +-----------------------------------------------------------+
// | Core 0: OS / Kernel / Interrupts                           |
// | Core 1: JVM / GC (Matching JVM PID=1)                     |
// +-----------------------------------------------------------+

// Process: PID 2 → Aeron Media Driver (external process)
// +-----------------------------------------------------------+
// | Core 2: Conductor thread                                   |
// | Core 3: Sender thread                                      |
// | Core 4: Receiver thread 1                                  |
// | Core 5: Receiver thread 2 (optional / high throughput)    |
// +-----------------------------------------------------------+

// Process: PID 1 → Matching Engine JVM
// +-----------------------------------------------------------+
// | Core 6: Matching shard 1                                   |
// | Core 7: Matching shard 2                                   |
// | Core 8: Matching shard 3                                   |
// | Core 9: Matching shard 4                                   |
// | Core 10: Matching shard 5                                  |
// | Core 11: Outbound messaging / persistence                 |
// +-----------------------------------------------------------+

/**
 * TODO:
 * - Separate the disruptor and set up 4-6 orderbooks per disruptor (Shard)
 * - Run one shard per thread (total 5 threads/shards)
 * - Separate process for an external Aeron media driver
 * - Setup messaging on separate thread
 */

import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.orderbook.OrderRequest;
import com.github.kkomitski.opal.utils.OpalConfig;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

public class IngressService {
  private final AeronSubscriber ingressSubscriber;
  private final IdleStrategy idleStrategy;
  private volatile boolean running = true;

  public IngressService(final AeronSubscriber ingressSubscriber) {
    this(ingressSubscriber, new BackoffIdleStrategy());
  }

  public IngressService(final AeronSubscriber ingressSubscriber, final IdleStrategy idleStrategy) {
    if (ingressSubscriber == null) {
      throw new IllegalArgumentException("ingressSubscriber must not be null");
    }
    if (idleStrategy == null) {
      throw new IllegalArgumentException("idleStrategy must not be null");
    }
    this.ingressSubscriber = ingressSubscriber;
    this.idleStrategy = idleStrategy;
  }

  public void subscribe(OrderBook[] orderBooks) {
    // Create fragment handler
    FragmentHandler handler = new OrderFragmentHandler(orderBooks);

    System.out.println("\n\n\u001B[32m" +
        "##########################\n" +
        "#      AERON HOT         #\n" +
        "##########################\n" +
        "\u001B[0m\n\n");

    try {
      while (running) {
        int fragmentsRead = ingressSubscriber.poll(handler, OpalConfig.AERON_FRAGMENT_LIMIT);
        idleStrategy.idle(fragmentsRead);
      }
    } finally {
      ingressSubscriber.close();
    }
  }

  public void stop() {
    running = false;
  }

  private static class OrderFragmentHandler implements FragmentHandler {
    private final OrderBook[] orderBooks;

    public OrderFragmentHandler(OrderBook[] orderBooks) {
      this.orderBooks = orderBooks;
    }

    // TODO: Find a way to get rid of this copy
    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
      // Process messages in 11-byte chunks
      int position = offset;
      while (position + OrderRequest.REQUEST_SIZE <= offset + length) {
        // Read instrument index from first 2 bytes
        int instrumentIndex = ((buffer.getByte(position) & 0xFF) << 8 | 
                               (buffer.getByte(position + 1) & 0xFF)) & 0x7FFF;

        if (instrumentIndex >= 0 && instrumentIndex < orderBooks.length) {
          orderBooks[instrumentIndex].publishOrder(buffer, position);
        } else {
          System.err.println("Invalid instrument index: " + instrumentIndex);
        }

        position += OrderRequest.REQUEST_SIZE;
      }
    }
  }
}
