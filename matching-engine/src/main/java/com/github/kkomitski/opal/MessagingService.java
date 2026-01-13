package com.github.kkomitski.opal;

import com.github.kkomitski.opal.utils.MatchEventDecoder;

public class MessagingService {
  // Dummy method to simulate emitting a message about an order match or
  // resolution
  public static void emitMessage(byte[] bytes) {
    // System.out.println("[MessagingService] " + bytes);
  }

  // Overload for structured messages (for future extensibility)
  public static void emitOrderMatch(byte[] encodedBytes) {
    // Disassemble and print the decoded fields
    int takerOrderId = MatchEventDecoder.getTakerOrderId(encodedBytes);
    int makerOrderId = MatchEventDecoder.getMakerOrderId(encodedBytes);
    int price = MatchEventDecoder.getPrice(encodedBytes);
    int quantity = MatchEventDecoder.getQuantity(encodedBytes);
    long timestamp = MatchEventDecoder.getTimestamp(encodedBytes);

    // String message = String.format(
    //     "[OrderMatch] takerOrderId=%d, makerOrderId=%d, price=%d, quantity=%d, timestamp=%d%n",
    //     takerOrderId, makerOrderId, price, quantity, timestamp);

    // // Write to file
    // try (BufferedWriter writer = new BufferedWriter(new FileWriter("order_matches.log", true))) {
    //   writer.write(message);
    // } catch (IOException e) {
    //   e.printStackTrace();
    // }

    emitMessage(encodedBytes);
  }
}