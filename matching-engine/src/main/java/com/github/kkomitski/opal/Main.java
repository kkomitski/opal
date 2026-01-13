package com.github.kkomitski.opal;

import java.io.IOException;

import com.github.kkomitski.opal.helpers.LoadOrderBooks;
import com.github.kkomitski.opal.services.TCPServer;

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

        System.out.println("Successfully loaded markets.xml");

        // UDP
        // AeronService aeronService = new AeronService();

        // try {
        //     aeronService.subscribe(orderBooks);
        // } catch (Exception e) {
        //     System.err.println("Failed to start server: " + e.getMessage());
        //     e.printStackTrace();
        //     System.exit(1);
        // }

        // TCP
        TCPServer tcpServer = new TCPServer();

        try {
            tcpServer.startServer(orderBooks);
        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        if (prometheusServer != null) {
            prometheusServer.close();
        }
    }
}