package com.github.kkomitski.opal.orderbook;

import org.agrona.DirectBuffer;

public class OrderRequest {
  public static enum RejectionReason {
    ORDERBOOK_FULL,
    PRICE_LIMIT_FULL,
    BID_PRICE_TOO_LOW,
    ASK_PRICE_TOO_HIGH,
    AVG_PRICE_MOVED_TOO_FAR
  }

  public static final int REQUEST_SIZE = 11;

  private static final int INSTRUMENT_MASK = 32767;
  private static final int BID_BIT_MASK = 0x8000;
  private static final int BYTE_MASK = 255;
  private static final int BYTE_SHIFT = 8;
  private static final int TWO_BYTE_SHIFT = 16;
  private static final int THREE_BYTE_SHIFT = 24;

  private static final int INSTRUMENT_INDEX_OFFSET = 0;
  private static final int PRICE_OFFSET = 2;
  private static final int QUANTITY_OFFSET = 5;
  private static final int ID_OFFSET = 7;

  private int instrumentIndex;
  private boolean bid;
  private int price;
  private int quantity;
  private int id;

  public OrderRequest() {
  }

  public int getInstrumentIndex() {
    return instrumentIndex;
  }

  public boolean isBid() {
    return bid;
  }

  public int getPrice() {
    return price;
  }

  public int getQuantity() {
    return quantity;
  }

  public int getId() {
    return id;
  }

  public void set(final int instrumentIndex, final boolean isBid, final int price, final int quantity, final int id) {
    this.instrumentIndex = instrumentIndex;
    this.bid = isBid;
    this.price = price;
    this.quantity = quantity;
    this.id = id;
  }

  public void setFromOrder(final Order order, final boolean isBid, final int price, final int instrumentIndex) {
    set(instrumentIndex, isBid, price, order.size, order.id);
  }

  public static int decodeInstrumentIndex(final DirectBuffer buffer, final int offset) {
    final int header = ((buffer.getByte(offset + INSTRUMENT_INDEX_OFFSET) & BYTE_MASK) << BYTE_SHIFT)
        | (buffer.getByte(offset + INSTRUMENT_INDEX_OFFSET + 1) & BYTE_MASK);
    return header & INSTRUMENT_MASK;
  }

  public static boolean decodeIsBid(final DirectBuffer buffer, final int offset) {
    final int header = ((buffer.getByte(offset + INSTRUMENT_INDEX_OFFSET) & BYTE_MASK) << BYTE_SHIFT)
        | (buffer.getByte(offset + INSTRUMENT_INDEX_OFFSET + 1) & BYTE_MASK);
    return (header & BID_BIT_MASK) != 0;
  }

  public static int decodePrice(final DirectBuffer buffer, final int offset) {
    return (buffer.getByte(offset + PRICE_OFFSET) & BYTE_MASK) << TWO_BYTE_SHIFT
        | (buffer.getByte(offset + PRICE_OFFSET + 1) & BYTE_MASK) << BYTE_SHIFT
        | (buffer.getByte(offset + PRICE_OFFSET + 2) & BYTE_MASK);
  }

  public static int decodeQuantity(final DirectBuffer buffer, final int offset) {
    return (buffer.getByte(offset + QUANTITY_OFFSET) & BYTE_MASK) << BYTE_SHIFT
        | (buffer.getByte(offset + QUANTITY_OFFSET + 1) & BYTE_MASK);
  }

  public static int decodeId(final DirectBuffer buffer, final int offset) {
    return (buffer.getByte(offset + ID_OFFSET) & BYTE_MASK) << THREE_BYTE_SHIFT
        | (buffer.getByte(offset + ID_OFFSET + 1) & BYTE_MASK) << TWO_BYTE_SHIFT
        | (buffer.getByte(offset + ID_OFFSET + 2) & BYTE_MASK) << BYTE_SHIFT
        | (buffer.getByte(offset + ID_OFFSET + 3) & BYTE_MASK);
  }
}