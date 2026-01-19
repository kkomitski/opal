package com.github.kkomitski.opal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.github.kkomitski.opal.orderbook.Limit;
import com.github.kkomitski.opal.orderbook.LimitChunkPool;
import com.github.kkomitski.opal.orderbook.OrderRequest;

public class LimitTest {

  @Test
  void testLimitResetRemovesAllOrdersAndChunks() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);

    // Add several orders to create multiple chunks
    for (int i = 0; i < 10; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(i, true, 100 + i, 10, 10);
      assertTrue(limit.addOrder(orderRequest), "Order should be added");
    }

    assertNotNull(limit.peek(), "Limit should have orders before reset");
    assertTrue(limit.getTotalVolume() > 0, "Limit should have volume before reset");

    // Reset the limit
    limit.reset();

    // After reset, all state should be cleared
    assertNull(limit.peek(), "Limit should have no orders after reset");
    assertEquals(0, limit.getTotalVolume(), "Limit should have zero volume after reset");

    // Add new orders after reset to ensure no contamination
    for (int i = 0; i < 5; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(i, true, 200 + i, 5, 5);
      assertTrue(limit.addOrder(orderRequest), "Order should be added after reset");
    }
    assertEquals(25, limit.getTotalVolume(), "Limit should correctly track new volume after reset");
  }

  @Test
  void testAddSingleOrder() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 7, 1);
    assertTrue(limit.addOrder(orderRequest), "Order should be added");
    assertEquals(7, limit.getTotalVolume(), "Total volume should match order size");
    assertNotNull(limit.peek(), "Should be able to peek order");
  }

  @Test
  void testAddAndRemoveOrder() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 8, 2);
    assertTrue(limit.addOrder(orderRequest), "Order should be added");
    assertEquals(8, limit.getTotalVolume(), "Total volume should match order size");
    limit.removeOrder();
    assertEquals(0, limit.getTotalVolume(), "Total volume should be zero after removal");
    assertNull(limit.peek(), "Should be empty after removal");
  }

  @Test
  void testResetOnEmptyLimit() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    limit.reset();
    assertEquals(0, limit.getTotalVolume(), "Total volume should be zero after reset");
    assertNull(limit.peek(), "Should be empty after reset");
  }

  @Test
  void testAddAfterReset() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    limit.reset();
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 9, 3);
    assertTrue(limit.addOrder(orderRequest), "Order should be added after reset");
    assertEquals(9, limit.getTotalVolume(), "Total volume should match order size after reset");
    assertNotNull(limit.peek(), "Should be able to peek order after reset");
  }

  @Test
  void testMultipleOrdersAccumulateVolume() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    int total = 0;
    for (int i = 0; i < 4; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100 + i, 2 + i, i);
      assertTrue(limit.addOrder(orderRequest));
      total += (2 + i);
    }
    assertEquals(total, limit.getTotalVolume(), "Total volume should be sum of all order sizes");
  }

  @Test
  void testRemoveAllOrdersEmptiesLimit() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    for (int i = 0; i < 3; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100, 5, i);
      limit.addOrder(orderRequest);
    }
    while (limit.peek() != null) {
      limit.removeOrder();
    }
    assertEquals(0, limit.getTotalVolume(), "Total volume should be zero after all removals");
    assertNull(limit.peek(), "Should be empty after all removals");
  }

  @Test
  void testPartialFillReducesVolume() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 10, 1);
    limit.addOrder(orderRequest);
    limit.partialFill(4);
    assertEquals(4, limit.getTotalVolume(), "Total volume should reflect partial fill");
  }

  @Test
  void testPeekDoesNotRemoveOrder() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 6, 1);
    limit.addOrder(orderRequest);
    assertNotNull(limit.peek(), "Peek should return an order");
    assertEquals(6, limit.getTotalVolume(), "Volume should remain after peek");
    assertNotNull(limit.peek(), "Peek should not remove order");
  }

  @Test
  void testAddRemoveAddSequence() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest1 = new OrderRequest();
    orderRequest1.set(1, true, 100, 3, 1);
    limit.addOrder(orderRequest1);
    limit.removeOrder();
    assertEquals(0, limit.getTotalVolume(), "Volume should be zero after remove");
    OrderRequest orderRequest2 = new OrderRequest();
    orderRequest2.set(1, true, 101, 4, 2);
    limit.addOrder(orderRequest2);
    assertEquals(4, limit.getTotalVolume(), "Volume should match new order after add");
    assertNotNull(limit.peek(), "Should be able to peek new order");
  }

  @Test
  void testResetAfterPartialFill() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    OrderRequest orderRequest = new OrderRequest();
    orderRequest.set(1, true, 100, 12, 1);
    limit.addOrder(orderRequest);
    limit.partialFill(7);
    limit.reset();
    assertEquals(0, limit.getTotalVolume(), "Volume should be zero after reset");
    assertNull(limit.peek(), "Should be empty after reset");
  }

  @Test
  void testMultipleResets() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    for (int cycle = 0; cycle < 3; cycle++) {
      for (int i = 0; i < 2; i++) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.set(1, true, 100 + i, 2 + i, i);
        limit.addOrder(orderRequest);
      }
      limit.reset();
      assertEquals(0, limit.getTotalVolume(), "Volume should be zero after reset in cycle " + cycle);
      assertNull(limit.peek(), "Should be empty after reset in cycle " + cycle);
    }
  }

  @Test
  void testChunkChaining() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    // Add more than 256 orders to trigger chunk chaining
    int orderCount = 300;
    int orderSize = 3;
    for (int i = 0; i < orderCount; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100 + i, orderSize, i);
      assertTrue(limit.addOrder(orderRequest), "Order " + i + " should be added");
    }
    assertEquals(orderCount * orderSize, limit.getTotalVolume(), "Total volume should be correct across chunks");
    assertNotNull(limit.peek(), "Should be able to peek order in chained chunks");
  }

  @Test
  void testRemoveOrdersAcrossChunks() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    // Add 300 orders to trigger chunk chaining
    int orderCount = 300;
    int orderSize = 2;
    for (int i = 0; i < orderCount; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100, orderSize, i);
      limit.addOrder(orderRequest);
    }
    // Remove half of them
    for (int i = 0; i < orderCount / 2; i++) {
      assertNotNull(limit.removeOrder(), "Should be able to remove order " + i);
    }
    assertEquals((orderCount - orderCount / 2) * orderSize, limit.getTotalVolume(), 
                 "Volume should be correct after removing across chunks");
  }

  @Test
  void testResetAfterChunkChaining() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    // Add 400 orders to create multiple chunks
    for (int i = 0; i < 400; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100 + i, 5, i);
      limit.addOrder(orderRequest);
    }
    assertEquals(2000, limit.getTotalVolume(), "Volume should be correct before reset");
    
    // Reset the limit
    limit.reset();
    
    // After reset, all state should be cleared
    assertEquals(0, limit.getTotalVolume(), "Volume should be zero after reset with multiple chunks");
    assertNull(limit.peek(), "Should be empty after reset with multiple chunks");
  }

  @Test
  void testAddAfterResetWithPreviousChaining() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    // Add 350 orders to create multiple chunks
    for (int i = 0; i < 350; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100, 4, i);
      limit.addOrder(orderRequest);
    }
    
    // Reset
    limit.reset();
    
    // Add new orders and verify clean state
    for (int i = 0; i < 10; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 200, 7, i);
      assertTrue(limit.addOrder(orderRequest), "Order should be added after reset with previous chaining");
    }
    assertEquals(70, limit.getTotalVolume(), "Volume should match new orders after reset");
  }

  @Test
  void testMultipleResetCyclesWithChaining() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    
    for (int cycle = 0; cycle < 3; cycle++) {
      // Add 280 orders to trigger chaining
      for (int i = 0; i < 280; i++) {
        OrderRequest orderRequest = new OrderRequest();
        orderRequest.set(1, true, 100, 2, i);
        limit.addOrder(orderRequest);
      }
      assertEquals(560, limit.getTotalVolume(), "Volume should be correct in cycle " + cycle);
      
      // Reset
      limit.reset();
      assertEquals(0, limit.getTotalVolume(), "Volume should be zero after reset in cycle " + cycle);
      assertNull(limit.peek(), "Should be empty after reset in cycle " + cycle);
    }
  }

  @Test
  void testPartialRemovalAcrossChunks() {
    LimitChunkPool chunkPool = new LimitChunkPool();
    Limit limit = new Limit(chunkPool);
    
    // Add 270 orders to span multiple chunks
    for (int i = 0; i < 270; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 100, 3, i);
      limit.addOrder(orderRequest);
    }
    
    // Remove 100 orders
    for (int i = 0; i < 100; i++) {
      limit.removeOrder();
    }
    
    assertEquals((270 - 100) * 3, limit.getTotalVolume(), "Volume should be correct after partial removal");
    
    // Add more orders
    for (int i = 0; i < 50; i++) {
      OrderRequest orderRequest = new OrderRequest();
      orderRequest.set(1, true, 101, 4, i + 270);
      limit.addOrder(orderRequest);
    }
    
    assertEquals((270 - 100) * 3 + 50 * 4, limit.getTotalVolume(), 
                 "Volume should be correct after add following partial removal");
  }
}