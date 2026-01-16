package com.github.kkomitski.opal.aeron;

import java.io.File;
import java.util.concurrent.ThreadFactory;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import net.openhft.affinity.AffinityLock;

public class Main {
    public static void main(String[] args) {
        // Set up the media driver directory (optional, default is
        // /dev/shm/aeron-<user>)
        String aeronDir = System.getProperty("user.dir") + "/shared-memory";
        System.out.println(aeronDir);
        System.setProperty("aeron.dir", aeronDir);

        // Clean up previous directory if exists (optional, for dev)
        File dir = new File(aeronDir);
        if (dir.exists()) {
            deleteDir(dir);
        }

        // Configure the media driver
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorThreadFactory(pinnedThreadFactory("aeron-conductor", 2))
            .senderThreadFactory(pinnedThreadFactory("aeron-sender", 3))
            .receiverThreadFactory(pinnedThreadFactory("aeron-receiver", 4))
                .dirDeleteOnStart(true)
                // Enable both UDP and IPC (default)
                .termBufferSparseFile(true)
                .spiesSimulateConnection(true)
                .dirDeleteOnShutdown(true);

        System.out.println("Starting Aeron Media Driver (standalone)...");
        System.out.println("Aeron directory: " + aeronDir);
        System.out.println("Listening for UDP and IPC channels.");

        try (MediaDriver ignore = MediaDriver.launch(ctx)) {
            System.out.println("Media Driver started. Press Ctrl+C to exit.");
            // Keep process alive
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Media Driver interrupted, shutting down.");
        }
    }

    private static ThreadFactory pinnedThreadFactory(final String threadName, final int cpuId) {
        return (runnable) -> {
            final Runnable wrapped = () -> {
                try (AffinityLock lock = AffinityLock.acquireLock(cpuId)) {
                    runnable.run();
                } catch (Throwable t) {
                    System.err.println("Failed to set CPU affinity for '" + threadName + "' to CPU " + cpuId + ": " + t);
                    runnable.run();
                }
            };

            final Thread thread = new Thread(wrapped);
            thread.setName(threadName);
            return thread;
        };
    }

    // Recursively delete directory
    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }
}