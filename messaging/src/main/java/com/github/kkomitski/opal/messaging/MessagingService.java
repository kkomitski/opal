package com.github.kkomitski.opal.messaging;

import java.nio.ByteOrder;

import com.github.kkomitski.opal.aeron.utils.AeronMediaDriver;
import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.utils.OpalConfig;

import io.aeron.logbuffer.FragmentHandler;
import net.openhft.affinity.AffinityLock;

public class MessagingService {
    public static void main(String[] args) {
        final Thread worker = new Thread(MessagingService::runLoop, "messaging-ipc-consumer");
        worker.setDaemon(false);
        worker.start();

        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // TODO: Add a ring buffer to swallow incoming messages
    private static void runLoop() {
        try (AffinityLock lock = AffinityLock.acquireLock(OpalConfig.MESSAGING_SERVICE_CORE);
             AeronMediaDriver mediaDriver = new AeronMediaDriver();
             AeronSubscriber subscriber = new AeronSubscriber(mediaDriver, "ipc", OpalConfig.MATCHER_EGRESS_STREAM_ID)) {

            final FragmentHandler handler = (buffer, offset, length, header) -> {
                // Protocol TBD.
                // For now, treat this as an opaque payload (e.g. raw OrderRequest bytes or any future event type).
                // TODO(Kafka): publish to Kafka after decoding protocol version/type.

                if (length >= 4) {
                    final int firstWord = buffer.getInt(offset, ByteOrder.BIG_ENDIAN);
                    System.out.println("IPC msg len=" + length + " firstWord=" + firstWord);
                } else {
                    System.out.println("IPC msg len=" + length);
                }
            };

            while (!Thread.currentThread().isInterrupted()) {
                final int fragments = subscriber.poll(handler, OpalConfig.AERON_FRAGMENT_LIMIT);
                if (fragments == 0) {
                    Thread.onSpinWait();
                }
            }
        }
    }
}