package com.github.kkomitski.opal.services.messaging;

import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.aeron.utils.AeronPublisher;
import com.github.kkomitski.opal.utils.MatchEventDecoder;

public final class AeronIpcMatchEventPublisher implements MatchEventPublisher {
  private static final ThreadLocal<UnsafeBuffer> TL_BUFFER =
      ThreadLocal.withInitial(() -> new UnsafeBuffer(new byte[0]));

  private final AeronPublisher publisher;

  public AeronIpcMatchEventPublisher(final AeronPublisher publisher) {
    if (publisher == null) {
      throw new IllegalArgumentException("publisher must not be null");
    }
    this.publisher = publisher;
  }

  @Override
  public void publishMatchEvent(final byte[] encodedMatchEvent) {
    if (encodedMatchEvent == null || encodedMatchEvent.length < MatchEventDecoder.SIZE) {
      return;
    }

    final UnsafeBuffer buffer = TL_BUFFER.get();
    buffer.wrap(encodedMatchEvent);

    // Extremely thin: single offer attempt; drop if back-pressured.
    // Matching threads must stay hot; do not block here.
    publisher.offer(buffer, 0, MatchEventDecoder.SIZE);
  }
}
