package com.github.kkomitski.opal.services.aeron;

import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.aeron.utils.AeronPublisher;
import com.github.kkomitski.opal.utils.MatchEventDecoder;

/**
 * Legacy compatibility shim.
 *
 * Messaging is handled by the standalone `messaging` module (separate process).
 * The matching engine should stay hot, so this implementation is intentionally thin:
 * it performs a single Aeron offer attempt and drops on backpressure.
 */
@Deprecated
public final class MessagingService {
  private static volatile AeronPublisher publisher;
  private static final ThreadLocal<UnsafeBuffer> TL_ENCODE_BUFFER =
      ThreadLocal.withInitial(() -> new UnsafeBuffer(new byte[0]));

  private MessagingService() {
  }

  public static void init(final AeronPublisher publisher) {
    if (publisher == null) {
      throw new IllegalArgumentException("publisher must not be null");
    }
    MessagingService.publisher = publisher;
  }

  public static void close() {
    publisher = null;
  }

  public static void emitOrderMatch(final byte[] encodedBytes) {
    final AeronPublisher pub = publisher;
    if (pub == null || encodedBytes == null || encodedBytes.length < MatchEventDecoder.SIZE) {
      return;
    }

    final UnsafeBuffer src = TL_ENCODE_BUFFER.get();
    src.wrap(encodedBytes);

    // Single attempt; drop if back-pressured.
    pub.offer(src, 0, MatchEventDecoder.SIZE);
  }
}