package com.github.kkomits.opal;

public class Main {
    private static final String HOST = "127.0.0.1";
    // private static final String HOST = "192.168.1.169";
    private static final int PORT = 42069; // Match the server port
    
    // Configuration
    private static final int NUM_CONNECTIONS = 100; // Number of concurrent connections
    private static final int ORDERS_PER_SECOND = 1_000_000; // Target orders per second across all connections
    private static final int TEST_DURATION_SECONDS = 60; // How long to run the test

    public static void main(String[] args) throws Exception {
        // Parse command line arguments if provided
        String host = args.length > 0 ? args[0] : HOST;
        int port = args.length > 1 ? Integer.parseInt(args[1]) : PORT;
        int connections = args.length > 2 ? Integer.parseInt(args[2]) : NUM_CONNECTIONS;
        int ordersPerSec = args.length > 3 ? Integer.parseInt(args[3]) : ORDERS_PER_SECOND;
        int duration = args.length > 4 ? Integer.parseInt(args[4]) : TEST_DURATION_SECONDS;
        
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║   OPAL Load Testing Client           ║");
        System.out.println("╚══════════════════════════════════════╝");
        System.out.println();
        System.out.println("Usage: java Main [host] [port] [connections] [orders/sec] [duration]");
        System.out.println();
        
        LoadTestClient client = new LoadTestClient(host, port, connections, ordersPerSec, duration);
        // client.start("http://192.168.1.170:8080/markets");
        client.start("http://localhost:8080/markets");
    }
}