package com.github.kkomitski.opal.services.messaging;

public interface MatchEventPublisher {
  void publishMatchEvent(byte[] encodedMatchEvent);

  static MatchEventPublisher noop() {
    return encodedMatchEvent -> {
      // intentionally no-op
    };
  }
}
