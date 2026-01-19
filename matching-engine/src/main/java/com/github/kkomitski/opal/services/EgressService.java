package com.github.kkomitski.opal.services;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.aeron.utils.AeronPublisher;

public class EgressService {
  private static final ThreadLocal<UnsafeBuffer> TL_BUFFER = ThreadLocal
      .withInitial(() -> new UnsafeBuffer(new byte[0]));

  private final AeronPublisher publisher;

  public EgressService() {
    this.publisher = null;
  }

  public EgressService(final AeronPublisher publisher) {
    this.publisher = publisher;
  }

  public void egress(final byte[] bytes, final int offset, final int length) {
    if (publisher == null || bytes == null || length <= 0) {
      return;
    }

    final UnsafeBuffer buffer = TL_BUFFER.get();
    buffer.wrap(bytes);

    // Extremely thin: single offer attempt; drop if back-pressured.
    // OrderBook must stay hot; do not block here.
    publisher.offer(buffer, offset, length);
  }

  public void egress(final DirectBuffer buffer, final int offset, final int length) {
    if (publisher == null || buffer == null || length <= 0) {
      return;
    }

    // Extremely thin: single offer attempt; drop if back-pressured.
    // OrderBook must stay hot; do not block here.
    publisher.offer(buffer, offset, length);
  }
}
