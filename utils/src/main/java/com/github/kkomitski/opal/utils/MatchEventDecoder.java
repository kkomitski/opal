package com.github.kkomitski.opal.utils;

public class MatchEventDecoder {
  // Field sizes (bytes): takerOrderId (4), makerOrderId (4), price (4), quantity
  // (4), timestamp (8)
  public static final int SIZE = 24;

  public static byte[] encode(int takerOrderId, int makerOrderId, int price, int quantity, long timestamp,
      byte[] buffer) {

    if (buffer == null) {
      buffer = new byte[SIZE];
    } 

    if(buffer.length != SIZE) {
      System.out.print("Buffer is of wrong size!");
    }
    // takerOrderId (4 bytes)
    buffer[0] = (byte) (takerOrderId >>> 24);
    buffer[1] = (byte) (takerOrderId >>> 16);
    buffer[2] = (byte) (takerOrderId >>> 8);
    buffer[3] = (byte) (takerOrderId);
    // makerOrderId (4 bytes)
    buffer[4] = (byte) (makerOrderId >>> 24);
    buffer[5] = (byte) (makerOrderId >>> 16);
    buffer[6] = (byte) (makerOrderId >>> 8);
    buffer[7] = (byte) (makerOrderId);
    // price (4 bytes)
    buffer[8] = (byte) (price >>> 24);
    buffer[9] = (byte) (price >>> 16);
    buffer[10] = (byte) (price >>> 8);
    buffer[11] = (byte) (price);
    // quantity (4 bytes)
    buffer[12] = (byte) (quantity >>> 24);
    buffer[13] = (byte) (quantity >>> 16);
    buffer[14] = (byte) (quantity >>> 8);
    buffer[15] = (byte) (quantity);
    // timestamp (8 bytes)
    buffer[16] = (byte) (timestamp >>> 56);
    buffer[17] = (byte) (timestamp >>> 48);
    buffer[18] = (byte) (timestamp >>> 40);
    buffer[19] = (byte) (timestamp >>> 32);
    buffer[20] = (byte) (timestamp >>> 24);
    buffer[21] = (byte) (timestamp >>> 16);
    buffer[22] = (byte) (timestamp >>> 8);
    buffer[23] = (byte) (timestamp);
    return buffer;
  }

  public static int getTakerOrderId(byte[] bytes) {
    return ((bytes[0] & 0xFF) << 24) |
        ((bytes[1] & 0xFF) << 16) |
        ((bytes[2] & 0xFF) << 8) |
        (bytes[3] & 0xFF);
  }

  public static int getMakerOrderId(byte[] bytes) {
    return ((bytes[4] & 0xFF) << 24) |
        ((bytes[5] & 0xFF) << 16) |
        ((bytes[6] & 0xFF) << 8) |
        (bytes[7] & 0xFF);
  }

  public static int getPrice(byte[] bytes) {
    return ((bytes[8] & 0xFF) << 24) |
        ((bytes[9] & 0xFF) << 16) |
        ((bytes[10] & 0xFF) << 8) |
        (bytes[11] & 0xFF);
  }

  public static int getQuantity(byte[] bytes) {
    return ((bytes[12] & 0xFF) << 24) |
        ((bytes[13] & 0xFF) << 16) |
        ((bytes[14] & 0xFF) << 8) |
        (bytes[15] & 0xFF);
  }

  public static long getTimestamp(byte[] bytes) {
    return ((long) (bytes[16] & 0xFF) << 56) |
        ((long) (bytes[17] & 0xFF) << 48) |
        ((long) (bytes[18] & 0xFF) << 40) |
        ((long) (bytes[19] & 0xFF) << 32) |
        ((long) (bytes[20] & 0xFF) << 24) |
        ((long) (bytes[21] & 0xFF) << 16) |
        ((long) (bytes[22] & 0xFF) << 8) |
        ((long) (bytes[23] & 0xFF));
  }

}
