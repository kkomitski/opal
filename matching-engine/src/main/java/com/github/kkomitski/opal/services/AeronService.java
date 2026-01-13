package com.github.kkomitski.opal.services;

// Removed incorrect import

import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.orderbook.OrderRequest;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

public class AeronService {
  private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
  private static final int STREAM_ID = 1001;
  private static final int FRAGMENT_LIMIT = 10;

  private MediaDriver mediaDriver;
  private Aeron aeron;
  private volatile boolean running = true;

  public void subscribe(OrderBook[] orderBooks) {
    // Start embedded media driver
    mediaDriver = MediaDriver.launchEmbedded();

    // Create Aeron instance
    aeron = Aeron.connect(new Aeron.Context()
        .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

    // Create subscription
    io.aeron.Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);

    // Create fragment handler
    FragmentHandler handler = new OrderFragmentHandler(orderBooks);

    // Idle strategy for when no messages are available
    IdleStrategy idleStrategy = new SleepingIdleStrategy(1000);

    System.out.println("Aeron server listening on " + CHANNEL + " (stream " + STREAM_ID + ")");

    System.out.println("\n\n\u001B[32m" +
        "##########################\n" +
        "#      AERON HOT         #\n" +
        "##########################\n" +
        "\u001B[0m\n\n");

    // Poll for messages
    while (running) {
      int fragmentsRead = subscription.poll(handler, FRAGMENT_LIMIT);
      idleStrategy.idle(fragmentsRead);
    }

    // Cleanup
    subscription.close();
    aeron.close();
    mediaDriver.close();
  }

  public void stop() {
    running = false;
  }

  private static class OrderFragmentHandler implements FragmentHandler {
    private final OrderBook[] orderBooks;

    public OrderFragmentHandler(OrderBook[] orderBooks) {
      this.orderBooks = orderBooks;
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
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
