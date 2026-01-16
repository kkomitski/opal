package com.github.kkomitski.opal;

import java.io.IOException;

import com.github.kkomitski.opal.aeron.utils.AeronMediaDriver;
import com.github.kkomitski.opal.aeron.utils.AeronPublisher;
import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.helpers.LoadOrderBooks;
import com.github.kkomitski.opal.services.aeron.IngressService;
import com.github.kkomitski.opal.services.messaging.AeronIpcMatchEventPublisher;
import com.github.kkomitski.opal.services.messaging.MatchEventPublisher;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

// Example nc requests
// AAPL - printf '\x80\x01\x01\x86\xa0\x00\x32' | nc 192.168.1.176 42069
// AAPL - printf '\x80\x01\x01\x86\xa0\x00\x32' | nc 192.168.1.176 42069 ..

public class Main {
    public static void main(String[] args) {
        HTTPServer prometheusServer = null;

        // JVM related metrics
        DefaultExports.initialize(); // Start Prometheus metrics HTTP server on port 9090 (or your preferred port)
        try {
            prometheusServer = new HTTPServer(9090);
        } catch (IOException e) {
            System.err.println("Failed to start Prometheus metrics server: " + e.getMessage());
            System.exit(1);
        }

        // Loads a static list of order books per instrument as defined in the market
        // data
        // OrderBook[] orderBooks =
        // LoadOrderBooks.fromXML("http://192.168.1.170:8080/markets");
        OrderBook[] orderBooks = LoadOrderBooks.fromXML("http://localhost:8080/markets");

        System.out.println("Successfully loaded markets.xml (books=" + orderBooks.length + ")");

        // Example DI wiring.
        // External MediaDriver process must already be running against the same
        // -Daeron.dir (or default).
        try (AeronMediaDriver aeronMediaDriver = new AeronMediaDriver()) {
            final AeronPublisher matchEventPublication = new AeronPublisher(aeronMediaDriver, "ipc", 2001);
            final MatchEventPublisher matchEventPublisher = new AeronIpcMatchEventPublisher(matchEventPublication);

            for (final OrderBook book : orderBooks) {
                book.setMatchEventPublisher(matchEventPublisher);
            }

            final AeronSubscriber ingressSubscriber = new AeronSubscriber(aeronMediaDriver, "udp", 2001, 42069);
            final IngressService aeronService = new IngressService(ingressSubscriber);
            aeronService.subscribe(orderBooks);
        }

        // try {
        // aeronService.subscribe(orderBooks);
        // } catch (Exception e) {
        // System.err.println("Failed to start server: " + e.getMessage());
        // e.printStackTrace();
        // System.exit(1);
        // }

        // TCP
        // TCPServer tcpServer = new TCPServer();

        // try {
        // tcpServer.startServer(orderBooks);
        // } catch (Exception e) {
        // System.err.println("Failed to start server: " + e.getMessage());
        // e.printStackTrace();
        // System.exit(1);
        // }

        if (prometheusServer != null) {
            prometheusServer.close();
        }
    }
}