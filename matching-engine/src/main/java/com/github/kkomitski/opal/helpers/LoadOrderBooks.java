package com.github.kkomitski.opal.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.agrona.concurrent.CachedEpochClock;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.services.EgressService;
import com.github.kkomitski.opal.utils.Market;
import com.github.kkomitski.opal.utils.MarketsLoader;
import com.github.kkomitski.opal.utils.OpalConfig;

import net.openhft.affinity.AffinityLock;

// TODO: Setup a single disruptor shards per 6-8 orderbooks and pin each shard to a CPU core
public class LoadOrderBooks {
  private static final CachedEpochClock EPOCH_CLOCK = new CachedEpochClock();
  private static final AtomicBoolean CLOCK_STARTED = new AtomicBoolean(false);

  public static OrderBook[] fromXML(String source, EgressService egressService) {
    startEpochClockThread();

    Market[] markets = MarketsLoader.load(source);
    List<OrderBook> orderBooks = new ArrayList<>();

    for (int i = 0; i < markets.length; i++) {
      Market market = markets[i];
      // Pass the dynamic sizing parameters from XML
      orderBooks.add(new OrderBook(
        market.symbol,
        i,
        market.limitsPerBook,
        market.ordersPerLimit,
        egressService,
        EPOCH_CLOCK
      ));
    }

    return orderBooks.toArray(new OrderBook[0]);
  }

  private static void startEpochClockThread() {
    if (!CLOCK_STARTED.compareAndSet(false, true)) {
      return;
    }

    final Thread clockThread = new Thread(() -> {
      try (AffinityLock lock = AffinityLock.acquireLock(OpalConfig.OS_CORE)) {
        while (!Thread.currentThread().isInterrupted()) {
          EPOCH_CLOCK.update(System.currentTimeMillis());
          LockSupport.parkNanos(1_000_000L); // ~1ms
        }
      }
    }, "opal-epoch-clock");
    clockThread.setDaemon(true);
    clockThread.start();
  }
}
