package com.github.kkomitski.opal.orderbook;

/**
 * Default pool size is 1000;
 */
public class LimitChunkPool {
    // Constants
    private static final int DEFAULT_POOL_SIZE = 1000;

    // Metadata
    private int count = 0;
    private final int poolSize;

    // Data
    private final LimitChunk[] pool;

    public LimitChunkPool() {
        this(DEFAULT_POOL_SIZE);
    }

    public LimitChunkPool(int poolSize) {
        this.poolSize = poolSize;
        pool = new LimitChunk[poolSize];

        for (int i = 0; i < poolSize; i++) {
            pool[i] = new LimitChunk();
        }
    }

    public LimitChunkPool(int poolSize, int ordersPerChunk) {
        this.poolSize = poolSize;
        pool = new LimitChunk[poolSize];

        for (int i = 0; i < poolSize; i++) {
            pool[i] = new LimitChunk(ordersPerChunk);
        }
    }

    public LimitChunk getChunk() {
        if (count == poolSize) {
            throw new LimitPoolExhaustedException("LimitChunk pool exhausted! Size: " + poolSize);
        }
        LimitChunk limitChunk = pool[count];
        count++;
        return limitChunk;
    }

    /**
     * Resets the chunk and releases it back into the pool
     * 
     * @param limitChunk
     */
    public void releaseChunk(LimitChunk limitChunk) {
        if (count == 0) {
            // throw new AllLimitsAlreadyReleasedException("All limits already released");
            System.err.println("All LimitChunk objects already released");
            return;
        }
        limitChunk.reset();
        count--;
        pool[count] = limitChunk;
    }

    public static class LimitPoolExhaustedException extends RuntimeException {
        public LimitPoolExhaustedException(String message) {
            super(message);
        }
    }

    public static class AllLimitsAlreadyReleasedException extends RuntimeException {
        public AllLimitsAlreadyReleasedException(String message) {
            super(message);
        }
    }

    public int getActiveCount() {
        return count;
    }

    public int getCapacity() {
        return pool.length;
    }
}
