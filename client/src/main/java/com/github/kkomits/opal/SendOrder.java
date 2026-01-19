package com.github.kkomits.opal;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.agrona.concurrent.UnsafeBuffer;

import com.github.kkomitski.opal.aeron.utils.AeronPublisher;
import com.github.kkomitski.opal.aeron.utils.AttachAeronMediaDriver;
import com.github.kkomitski.opal.utils.OpalConfig;
import com.github.kkomitski.opal.utils.OrderRequestDecoder;

public class SendOrder {

	private static final int INSTRUMENT_INDEX = 0;

	public static void main(String[] args) throws Exception {
		// Dead simple: publish to matcher ingress UDP endpoint on localhost.
		System.setProperty(
				AeronPublisher.UDP_ENDPOINT_PROP,
				"localhost:" + OpalConfig.MATCHER_INGRESS_PORT);

		try (AttachAeronMediaDriver mediaDriver = new AttachAeronMediaDriver();
				AeronPublisher publisher = new AeronPublisher(mediaDriver, "udp", OpalConfig.MATCHER_INGRESS_STREAM_ID);
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

			System.out.println("SendOrder ready. Commands:");
			System.out.println("  market buy <qty>");
			System.out.println("  market sell <qty>");
			System.out.println("  limit buy <qty> <price>");
			System.out.println("  limit sell <qty> <price>");
			System.out.println("Type 'quit' to exit.");

			int orderId = 1;
			while (true) {
				System.out.print("> ");
				final String line = reader.readLine();
				if (line == null) {
					break;
				}

				final String trimmed = line.trim();
				if (trimmed.isEmpty()) {
					continue;
				}

				if ("quit".equalsIgnoreCase(trimmed) || "exit".equalsIgnoreCase(trimmed)) {
					break;
				}

				try {
					final String[] parts = trimmed.split("\\s+");
					if (parts.length < 3) {
						throw new IllegalArgumentException("Not enough args");
					}

					final String type = parts[0].toLowerCase();
					final String side = parts[1].toLowerCase();
					final boolean isBid;
					if ("buy".equals(side)) {
						isBid = true;
					} else if ("sell".equals(side)) {
						isBid = false;
					} else {
						throw new IllegalArgumentException("Side must be buy|sell");
					}

					final int quantity = Integer.parseInt(parts[2]);
					if (quantity <= 0 || quantity > 0xFFFF) {
						throw new IllegalArgumentException("qty must be 1..65535");
					}

					final int price;
					if ("market".equals(type)) {
						price = 0;
						if (parts.length != 3) {
							throw new IllegalArgumentException("market expects: market buy|sell <qty>");
						}
					} else if ("limit".equals(type)) {
						if (parts.length != 4) {
							throw new IllegalArgumentException("limit expects: limit buy|sell <qty> <price>");
						}
						price = Integer.parseInt(parts[3]);
						if (price <= 0) {
							throw new IllegalArgumentException("price must be > 0 for limit orders");
						}
					} else {
						throw new IllegalArgumentException("Type must be market|limit");
					}

					final byte[] bytes = OrderRequestDecoder.encode(
							INSTRUMENT_INDEX,
							price,
							(short) quantity,
							isBid,
							orderId++);

					final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
					final long result = publisher.offer(buffer, 0, bytes.length);
					if (result < 0) {
						System.out.println("offer failed: " + result);
					} else {
						System.out.println("sent (instrument=" + INSTRUMENT_INDEX + ")");
					}
				} catch (Exception e) {
					System.out.println("ERR: " + e.getMessage());
				}
			}
		}
	}
}
