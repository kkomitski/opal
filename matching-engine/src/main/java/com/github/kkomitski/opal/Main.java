package com.github.kkomitski.opal;

import java.io.IOException;

import com.github.kkomitski.opal.aeron.utils.AeronMediaDriver;
import com.github.kkomitski.opal.aeron.utils.AeronPublisher;
import com.github.kkomitski.opal.aeron.utils.AeronSubscriber;
import com.github.kkomitski.opal.helpers.LoadOrderBooks;
import com.github.kkomitski.opal.services.EgressService;
import com.github.kkomitski.opal.services.aeron.IngressService;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

// Example nc requests
// AAPL - printf '\x80\x01\x01\x86\xa0\x00\x32' | nc 192.168.1.176 42069
// AAPL - printf '\x80\x01\x01\x86\xa0\x00\x32' | nc 192.168.1.176 42069 ..

public class Main {
    public static void main(String[] args) {
        
        
        // JVM related metrics
        HTTPServer prometheusServer = null;
        try {
            DefaultExports.initialize(); 
            prometheusServer = new HTTPServer(9090);
        } catch (IOException e) {
            System.err.println("Failed to start Prometheus metrics server: " + e.getMessage());
            System.exit(1);
        }
        
        // Start Aeron communications
        try (AeronMediaDriver aeronMediaDriver = new AeronMediaDriver()) {
            final AeronPublisher egressPublication = new AeronPublisher(aeronMediaDriver, "ipc", 2001);
            final EgressService egressService = new EgressService(egressPublication);
            
            // Loads a static list of order books per instrument as defined in the markets XML
            OrderBook[] orderBooks = LoadOrderBooks.fromXML("http://localhost:8080/markets", egressService);
            System.out.println("Successfully loaded markets.xml (books=" + orderBooks.length + ")");

            final AeronSubscriber ingressSubscriber = new AeronSubscriber(aeronMediaDriver, "udp", 2001, 42069);
            final IngressService ingressService = new IngressService(ingressSubscriber);
            ingressService.subscribe(orderBooks);
        }

        if (prometheusServer != null) {
            prometheusServer.close();
        }
    }
}