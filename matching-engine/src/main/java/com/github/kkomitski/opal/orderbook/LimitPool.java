/*
TODO: Possibly look into allowing for a parameter passed to control the chunk pool also
*/
package com.github.kkomitski.opal.orderbook;

public class LimitPool {
  private static final int DEFAULT_POOL_SIZE = 300;
  public final Limit[] pool;
  public int count = 0;
  private LimitChunkPool limitChunkPool;

  public LimitPool() {
    this(DEFAULT_POOL_SIZE, Limit.DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public LimitPool(int poolSize) {
    this(poolSize, Limit.DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public LimitPool(int limitPoolSize, int limitChunkPoolSize) {
    this(new LimitChunkPool(limitChunkPoolSize), limitPoolSize, Limit.DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public LimitPool(int limitPoolSize, int limitChunkPoolSize, int maxChunksPerLevel) {
    this(new LimitChunkPool(limitChunkPoolSize), limitPoolSize, maxChunksPerLevel);
  }

  public LimitPool(LimitChunkPool limitChunkPool) {
    this(limitChunkPool, DEFAULT_POOL_SIZE, Limit.DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public LimitPool(LimitChunkPool limitChunkPool, int poolSize) {
    this(limitChunkPool, poolSize, Limit.DEFAULT_MAX_CHUNKS_PER_LIMIT);
  }

  public LimitPool(LimitChunkPool limitChunkPool, int poolSize, int maxChunksPerLevel) {
    this.limitChunkPool = limitChunkPool;
    this.pool = new Limit[poolSize];
    for (int i = 0; i < poolSize; i++) {
      pool[i] = new Limit(limitChunkPool, maxChunksPerLevel);
    }
  }

  public Limit getLimit() {
    if (count == pool.length) {
      throw new IllegalStateException("Limit pool exhausted! Max limits: " + pool.length);
    }
    Limit limit = pool[count];
    count++;
    limit.initialized = true;
    return limit;
  }

  public void releaseLimit(Limit limit) {
    if (count == 0) {
      System.err.println("All Limit objects already released");
    }
    // System.out.print("Releasing a limit");
    count--;
    pool[count] = limit;
    limit.reset();
  }

  public int getActiveCount() {
    return count;
  }

  public int getCapacity() {
    return pool.length;
  }

  public LimitChunkPool getLimitChunkPool() {
    return limitChunkPool;
  }
}
