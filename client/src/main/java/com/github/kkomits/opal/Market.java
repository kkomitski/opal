package com.github.kkomits.opal;

public class Market {
    public String symbol;
    public int price;
    public int book_depth;
    public int level_depth;

    public Market(String symbol, int price, int book_depth, int level_depth) {
        this.symbol = symbol;
        this.price = price;
        this.book_depth = book_depth;
        this.level_depth = level_depth;
    }
}
