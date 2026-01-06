package com.github.kkomitski.opal.utils;

public class OrderRequestDecoder {
  // TODO: Possible performance could be gained by going to a power of 2 - 12 or 16 bytes (needs testing)
  public static final int SIZE = 11;

  // Bit masks
  private static final int INSTRUMENT_MASK = 0x7FFF;
  private static final int bid_BIT_MASK = 0x80;
  private static final int BYTE_MASK = 0xFF;

  // Bit shift amounts
  private static final int bid_BIT_SHIFT = 15;
  private static final int BYTE_SHIFT = 8;
  private static final int TWO_BYTE_SHIFT = 16;
  private static final int THREE_BYTE_SHIFT = 24;

  public static byte[] encode(
      int instrumentIndex,
      int price,
      short quantity,
      boolean isBid,
      int orderId) {
    byte[] bytes = new byte[SIZE];

    // First 2 bytes: The MSB for bid/ask and 15 bits for instrument index
    // (32,767 total)
    int header = (((isBid ? 1 : 0) << bid_BIT_SHIFT) | (instrumentIndex & INSTRUMENT_MASK));
    bytes[0] = (byte) (header >>> BYTE_SHIFT);
    bytes[1] = (byte) header;

    // Next 3 bytes: price (24 bits) (16,777,215 total)
    // Price of 0 is a Market Order
    bytes[2] = (byte) (price >>> TWO_BYTE_SHIFT);
    bytes[3] = (byte) (price >>> BYTE_SHIFT);
    bytes[4] = (byte) price;

    // Next 2 bytes: quantity (16 bits) (65,535 total)
    bytes[5] = (byte) (quantity >>> BYTE_SHIFT);
    bytes[6] = (byte) quantity;

    // Next 4 bytes - order id
    bytes[7] = (byte) (orderId >>> THREE_BYTE_SHIFT);
    bytes[8] = (byte) (orderId >>> TWO_BYTE_SHIFT);
    bytes[9] = (byte) (orderId >>> BYTE_SHIFT);
    bytes[10] = (byte) (orderId);

    return bytes;
  }

  public static int getInstrumentIndex(byte[] bytes) {
    return (((bytes[0] & BYTE_MASK) << BYTE_SHIFT) | (bytes[1] & BYTE_MASK)) & INSTRUMENT_MASK;
  }

  public static boolean isBid(byte[] bytes) {
    return (bytes[0] & bid_BIT_MASK) != 0;
  }

  public static int getPrice(byte[] bytes) {
    return ((bytes[2] & BYTE_MASK) << TWO_BYTE_SHIFT) |
        ((bytes[3] & BYTE_MASK) << BYTE_SHIFT) |
        (bytes[4] & BYTE_MASK);
  }

  public static int getQuantity(byte[] bytes) {
    return ((bytes[5] & BYTE_MASK) << BYTE_SHIFT) | (bytes[6] & BYTE_MASK);
  }

  public static int getId(byte[] bytes) {
    return ((bytes[7] & BYTE_MASK) << THREE_BYTE_SHIFT) |
        ((bytes[8] & BYTE_MASK) << TWO_BYTE_SHIFT) |
        ((bytes[9] & BYTE_MASK) << BYTE_SHIFT) |
        (bytes[10] & BYTE_MASK);
  }
}
