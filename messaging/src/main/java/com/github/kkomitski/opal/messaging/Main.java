package com.github.kkomitski.opal.messaging;

import java.nio.ByteOrder;

import com.github.kkomitski.opal.aeron.utils.AeronMediaDriver;
import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.utils.MatchEventDecoder;

import io.aeron.logbuffer.FragmentHandler;
import net.openhft.affinity.AffinityLock;

public class Main {
    private static final int WORKER_CORE_ID = 5;
    private static final int MATCH_EVENT_STREAM_ID = 2001;
    private static final int FRAGMENT_LIMIT = 10;

    public static void main(String[] args) {
        final Thread worker = new Thread(Main::runLoop, "messaging-ipc-consumer");
        worker.setDaemon(false);
        worker.start();

        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runLoop() {
        try (AffinityLock lock = AffinityLock.acquireLock(WORKER_CORE_ID);
             AeronMediaDriver mediaDriver = new AeronMediaDriver();
             AeronSubscriber subscriber = new AeronSubscriber(mediaDriver, "ipc", MATCH_EVENT_STREAM_ID)) {

            final FragmentHandler handler = (buffer, offset, length, header) -> {
                if (length < MatchEventDecoder.SIZE) {
                    return;
                }

                final int takerOrderId = buffer.getInt(offset, ByteOrder.BIG_ENDIAN);
                final int makerOrderId = buffer.getInt(offset + 4, ByteOrder.BIG_ENDIAN);
                final int price = buffer.getInt(offset + 8, ByteOrder.BIG_ENDIAN);
                final int quantity = buffer.getInt(offset + 12, ByteOrder.BIG_ENDIAN);
                final long timestamp = buffer.getLong(offset + 16, ByteOrder.BIG_ENDIAN);

                // TODO(Kafka): publish to Kafka topic here.
                // e.g. producer.send(new ProducerRecord<>("matches", key, serializedPayload));
                // Keep this handler allocation-free and fast.

                System.out.println("MATCH taker=" + takerOrderId +
                        " maker=" + makerOrderId +
                        " px=" + price +
                        " qty=" + quantity +
                        " ts=" + timestamp);
            };

            while (!Thread.currentThread().isInterrupted()) {
                final int fragments = subscriber.poll(handler, FRAGMENT_LIMIT);
                if (fragments == 0) {
                    Thread.onSpinWait();
                }
            }
        }
    }
}