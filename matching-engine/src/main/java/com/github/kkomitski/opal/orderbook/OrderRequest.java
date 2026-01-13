package com.github.kkomitski.opal.orderbook;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

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
  private static final int BID_BIT_MASK = 128;
  private static final int BYTE_MASK = 255;
  private static final int BYTE_SHIFT = 8;
  private static final int TWO_BYTE_SHIFT = 16;
  private static final int THREE_BYTE_SHIFT = 24;

  private static final int INSTRUMENT_INDEX_OFFSET = 0;
  private static final int PRICE_OFFSET = 2;
  private static final int QUANTITY_OFFSET = 5;
  private static final int ID_OFFSET = 7;

  public final DirectBuffer buffer;

  public OrderRequest() {
    this.buffer = new UnsafeBuffer(new byte[REQUEST_SIZE]);
  }

  public OrderRequest(DirectBuffer buffer) {
    this.buffer = buffer;
  }

  public int getInstrumentIndex() {
    int header = ((buffer.getByte(INSTRUMENT_INDEX_OFFSET) & BYTE_MASK) << BYTE_SHIFT) |
                 (buffer.getByte(INSTRUMENT_INDEX_OFFSET + 1) & BYTE_MASK);
    return header & INSTRUMENT_MASK;
  }

  public boolean isBid() {
    return (buffer.getByte(INSTRUMENT_INDEX_OFFSET) & BID_BIT_MASK) != 0;
  }

  public int getPrice() {
    return (buffer.getByte(PRICE_OFFSET) & BYTE_MASK) << TWO_BYTE_SHIFT |
           (buffer.getByte(PRICE_OFFSET + 1) & BYTE_MASK) << BYTE_SHIFT |
           (buffer.getByte(PRICE_OFFSET + 2) & BYTE_MASK);
  }

  public int getQuantity() {
    return (buffer.getByte(QUANTITY_OFFSET) & BYTE_MASK) << BYTE_SHIFT |
           (buffer.getByte(QUANTITY_OFFSET + 1) & BYTE_MASK);
  }

  public int getId() {
    return (buffer.getByte(ID_OFFSET) & BYTE_MASK) << THREE_BYTE_SHIFT |
           (buffer.getByte(ID_OFFSET + 1) & BYTE_MASK) << TWO_BYTE_SHIFT |
           (buffer.getByte(ID_OFFSET + 2) & BYTE_MASK) << BYTE_SHIFT |
           (buffer.getByte(ID_OFFSET + 3) & BYTE_MASK);
  }

  public void setBytes(byte[] src) {
    if (buffer instanceof UnsafeBuffer) {
      byte[] dest = ((UnsafeBuffer) buffer).byteArray();
      System.arraycopy(src, 0, dest, 0, src.length);
    }
  }

  public void setFromOrder(Order order, OrderRequest orderRequestBuffer, boolean isBid, int price, int instrumentIndex) {
    byte[] dest = ((UnsafeBuffer) this.buffer).byteArray();

    // First 2 bytes: The MSB for bid/ask and 15 bits for instrument index (32,767 total)
    int header = (((isBid ? 1 : 0) << 15) | (instrumentIndex & 32767));
    dest[0] = (byte) (header >>> 8);
    dest[1] = (byte) header;

    // Next 3 bytes: price (24 bits) (16,777,215 total)
    // Price of 0 is a Market Order
    dest[2] = (byte) (price >>> 16);
    dest[3] = (byte) (price >>> 8);
    dest[4] = (byte) price;

    // Next 2 bytes: quantity (16 bits) (65,535 total)
    dest[5] = (byte) (order.size >>> 8);
    dest[6] = (byte) order.size;

    // Next 4 bytes: order id
    dest[7] = (byte) (order.id >>> 24);
    dest[8] = (byte) (order.id >>> 16);
    dest[9] = (byte) (order.id >>> 8);
    dest[10] = (byte) order.id;
  }
}