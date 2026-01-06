package com.github.kkomitski.opal.utils;

public class Market {
    public final String symbol;
    public final int price;
    public final int limitsPerBook;
    public final int ordersPerLimit;

    public Market(String symbol, int price, int limitsPerBook, int ordersPerLimit) {
        this.symbol = symbol;
        this.price = price;
        this.limitsPerBook = limitsPerBook;
        this.ordersPerLimit = ordersPerLimit;
    }
}
