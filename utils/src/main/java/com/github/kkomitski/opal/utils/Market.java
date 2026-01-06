package com.github.kkomitski.opal.utils;

public class Market {
    public final String symbol;
    public final int price;
    public final int book_depth;
    public final int level_depth;

    public Market(String symbol, int price, int book_depth, int level_depth) {
        this.symbol = symbol;
        this.price = price;
        this.book_depth = book_depth;
        this.level_depth = level_depth;
    }
}
