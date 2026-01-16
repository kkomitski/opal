package com.github.kkomitski.opal.messaging;

import java.nio.ByteOrder;

import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.aeron.utils.AttachAeronMediaDriver;
import com.github.kkomitski.opal.utils.MatchEventDecoder;
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
             AttachAeronMediaDriver mediaDriver = new AttachAeronMediaDriver();
             AeronSubscriber subscriber = new AeronSubscriber(mediaDriver, "ipc", OpalConfig.MATCHER_EGRESS_STREAM_ID)) {

            final FragmentHandler handler = (buffer, offset, length, header) -> {
                if (length == MatchEventDecoder.SIZE) {
                    // final int takerOrderId = buffer.getInt(offset + MatchEventDecoder.TAKER_ORDER_ID_OFFSET, ByteOrder.BIG_ENDIAN);
                    // final int makerOrderId = buffer.getInt(offset + MatchEventDecoder.MAKER_ORDER_ID_OFFSET, ByteOrder.BIG_ENDIAN);
                    // final int price = buffer.getInt(offset + MatchEventDecoder.PRICE_OFFSET, ByteOrder.BIG_ENDIAN);
                    // final int quantity = buffer.getInt(offset + MatchEventDecoder.QUANTITY_OFFSET, ByteOrder.BIG_ENDIAN);
                    // final long timestamp = buffer.getLong(offset + MatchEventDecoder.TIMESTAMP_OFFSET, ByteOrder.BIG_ENDIAN);
                    byte[] bytes = buffer.byteArray();

                    final int takerOrderId = MatchEventDecoder.getTakerOrderId(bytes);
                    final int makerOrderId = MatchEventDecoder.getMakerOrderId(bytes); 
                    final int price = MatchEventDecoder.getPrice(bytes);
                    final int quantity = MatchEventDecoder.getQuantity(bytes);
                    final long timestamp = MatchEventDecoder.getTimestamp(bytes);

                    System.out.println(
                            "MATCH taker=" + takerOrderId +
                            " maker=" + makerOrderId +
                            " price=" + price +
                            " qty=" + quantity +
                            " ts=" + timestamp);
                    return;
                }

                // Unknown payload; best-effort debug print.
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